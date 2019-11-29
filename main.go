package main

import (
	"context"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

func main() {
	println("创建")
	dialer := NewPodForwarder()
	v := version.Version{Major: 6, Minor: 8, Patch: 4, Label: "",}
	ctx := context.Background()

	url := "http://elasticsearch-sample-es-http.nes-elasticsearch.svc:9200"
	url = "http://elasticsearch-sample-es-data-0.nes-elasticsearch.pod:9200"
	elasticsearchClient := esclient.NewElasticsearchClient(dialer, url, client.UserAuth{}, v, nil)

	println("开始请求")
	info, err := elasticsearchClient.GetClusterInfo(ctx)

	if err != nil {
		println(err)
	}
	println("========")
	println(info.ClusterName)
}


