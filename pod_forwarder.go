package main

import (
	"context"
	"errors"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"strings"
	"sync"
)

const syntheticDNSSegment = "pod"
var (
	log = logf.Log.WithName("portforward")
)

type PodForwarder struct {
	network, addr string

	initChan chan struct{}

	clientset *kubernetes.Clientset

	podNSN        types.NamespacedName

	viaErr error
	// viaAddr 重定向连接的地址
	viaAddr string
}

func NewPodForwarder() *PodForwarder {
	return &PodForwarder{
		initChan: make(chan struct{}),
	}
}

func (d *PodForwarder) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {

	d.addr = addr
	d.network = network


	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	d.clientset = clientset

	podNSN, err := parsePodAddr(addr, clientset)
	d.podNSN = *podNSN

	go func() {
		if err := d.Run(context.Background()); err != nil { // forwarder的具体run方法也要自己实现
			log.Error(err, "Forwarder returned with an error", "addr", addr)
		} else {
			log.Info("Forwarder returned without an error", "addr", addr)
		}
	}()

	select {
	case <-d.initChan:
	case <-ctx.Done():
	}

	// context has an error, so we can give up, most likely exceeded our timeout
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// we have an error to return
	if d.viaErr != nil {
		return nil, d.viaErr
	}

	log.V(1).Info("Redirecting dial call", "addr", d.addr, "via", d.viaAddr)
	var dialer net.Dialer
	return dialer.DialContext(ctx, d.network, d.viaAddr)
}

// Run 启动端口转发并阻塞，直到port forwarding失败或context is done.
func (f *PodForwarder) Run(ctx context.Context) error {
	log.Info("Running port-forwarder for", "addr", f.addr)
	defer log.Info("No longer running port-forwarder for", "addr", f.addr)

	// 确保我们只关闭init channel一次
	initCloser := sync.Once{}

	// wrap this in a sync.Once because it will panic if it happens more than once
	// ensure that initChan is closed even if we were never ready.
	defer initCloser.Do(func() {
		close(f.initChan)
	})

	// derive a new context so we can ensure the port-forwarding is stopped before we return and that we return as
	// soon as the port-forwarding stops, whichever occurs first
	runCtx, runCtxCancel := context.WithCancel(ctx)
	defer runCtxCancel()

	_, port, err := net.SplitHostPort(f.addr)
	if err != nil {
		return err
	}

	// 获取到本地的空闲端口
	localPort, err := GetRandomPort()
	if err != nil {
		return err
	}

	readyChan := make(chan struct{})
	fwd, err := newKubectlPortForwarder(runCtx, f.podNSN.Namespace, f.podNSN.Name,
		[]string{localPort + ":" + port}, readyChan)
	if err != nil {
		return err
	}

	// 等待context.Done()，或者port forwarder已经准备好
	go func() {
		select {
		case <-runCtx.Done():
		case <-readyChan:
			f.viaAddr = "127.0.0.1:" + localPort

			log.Info("Ready to redirect connections", "addr", f.addr, "via", f.viaAddr)

			// wrap this in a sync.Once because it will panic if it happens more than once, which it may if our
			// outer function returned just as readyChan was closed.
			initCloser.Do(func() {
				close(f.initChan)
			})
		}
	}()

	err = fwd.ForwardPorts()
	f.viaErr = errors.New("not currently forwarding")
	return err
}

func GetRandomPort() (string, error) {
	// 传入127.0.0.1:0输出一个随机端口
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	if err := listener.Close(); err != nil {
		return "", err
	}

	_, localPort, err := net.SplitHostPort(listener.Addr().String())
	return localPort, err
}

// podDNSRegex matches pods FQDN such as {name}.{namespace}.pod
var podDNSRegex = regexp.MustCompile(`^.+\..+$`)

// podIPRegex matches any ipv4 address.
var podIPv4Regex = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)

// parsePodAddr 从地址中转换出pod的名称和命名空间
func parsePodAddr(addr string, clientSet *kubernetes.Clientset) (*types.NamespacedName, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if podIPv4Regex.MatchString(host) {
		// we got an IP address
		// try to map it to a pod name and namespace
		return getPodWithIP(host, clientSet)
	}
	if podDNSRegex.MatchString(host) {
		// retrieve pod name and namespace from addr
		parts := strings.SplitN(host, ".", 4)
		if len(parts) <= 1 {
			return nil, fmt.Errorf("unsupported pod address format: %s", host)
		}
		if len(parts) == 2 || parts[2] == syntheticDNSSegment {
			// podname.ns[.pod] from service forwarder or direct call
			return &types.NamespacedName{Namespace: parts[1], Name: parts[0]}, nil
		}
		// podname.subdomain.ns
		return &types.NamespacedName{Namespace: parts[2], Name: parts[0]}, nil

	}
	return nil, fmt.Errorf("unsupported pod address format: %s", host)
}

// getPodWithIP requests the apiserver for pods with the given IP assigned.
func getPodWithIP(ip string, clientSet *kubernetes.Clientset) (*types.NamespacedName, error) {
	pods, err := clientSet.CoreV1().
		Pods("").
		List(metav1.ListOptions{
			FieldSelector: fmt.Sprintf("status.podIP=%s", ip),
		})
	if err != nil {
		return nil, err
	}
	if pods == nil || len(pods.Items) == 0 {
		return nil, fmt.Errorf("pod with IP %s not found", ip)
	}
	nsn := ExtractNamespacedName(&(pods.Items[0].ObjectMeta))
	return &nsn, nil
}

func ExtractNamespacedName(object metav1.Object) types.NamespacedName {
	return types.NamespacedName{
		Namespace: object.GetNamespace(),
		Name:      object.GetName(),
	}
}

func newKubectlPortForwarder(
	ctx context.Context,
	namespace, podName string,
	ports []string,
	readyChan chan struct{},
) (*portforward.PortForwarder, error) {
	println("---client_portforward-newKubectlPortForwarder---")
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}

	clientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	req := clientSet.RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward")

	u := url.URL{
		Scheme:   req.URL().Scheme,
		Host:     req.URL().Host,
		Path:     "/api/v1" + req.URL().Path,
		RawQuery: "timeout=32s",
	}

	// RoundTripperFor就是配置不同的transport来配置行为
	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, err
	}

	// 准备好拨号
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, &u)

	// wrap stdout / stderr through logging
	w := &logWriter{keysAndValues: []interface{}{
		"namespace", namespace,
		"pod", podName,
		"ports", ports,
	}}
	// 调用client-go的portforward拨号
	return portforward.New(dialer, ports, ctx.Done(), readyChan, w, w)
}

// logWriter is a small utility that writes data from an io.Writer to a log
type logWriter struct {
	keysAndValues []interface{}
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	log.Info(strings.TrimSpace(string(p)), w.keysAndValues...)

	return len(p), nil
}
