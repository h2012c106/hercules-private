package com.xiaohongshu.db.hercules.elasticsearchv6.schema.manager;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ElasticsearchManager {

    protected RestHighLevelClient client;
    protected final String docType;

    public ElasticsearchManager(String endpoint, int port, String docType) {
//        BulkProcessor processor = BulkProcessor.builder().build()
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(endpoint, port, "http"))
                .setRequestConfigCallback(
                        new RestClientBuilder.RequestConfigCallback() {
                            @Override
                            public RequestConfig.Builder customizeRequestConfig(
                                    RequestConfig.Builder requestConfigBuilder) {
                                return requestConfigBuilder
                                        .setConnectTimeout(30000)
                                        .setSocketTimeout(60000*5);
                            }
                        }));
        this.docType = docType;
    }

    // 目前仅支持upsert语义，后续若有需求，再加。
    public void doUpsert(List<DocRequest> docRequests) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (DocRequest docRequest : docRequests) {
            Map<String, Object> doc = docRequest.getDoc();
            IndexRequest indexRequest = new IndexRequest().index(docRequest.index).type(this.docType).id(docRequest.id).source(doc);
            UpdateRequest updateRequest = new UpdateRequest().index(docRequest.index).type(this.docType).id(docRequest.id)
                    .doc(doc)
                    .upsert(indexRequest);
            bulkRequest.add(updateRequest);
        }
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (response.hasFailures()){
            throw new RuntimeException("Bulk insert failed."+bulkRequest.getDescription());
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
