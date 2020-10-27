package com.xiaohongshu.db.hercules.elasticsearch.schema.manager;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ElasticsearchManager {

    private RestHighLevelClient client;
    private final String docType;

    public ElasticsearchManager(String endpoint, int port, String docType) {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(endpoint, port, "http")));
        this.docType = docType;
    }

    // 目前仅支持upsert语义，后续若有需求，再加。
    public void doUpsert(List<DocRequest> docRequests) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (DocRequest docRequest : docRequests) {
            Map<String, Object> doc = docRequest.getDoc();
            IndexRequest indexRequest = new IndexRequest(docRequest.index, docRequest.type, docRequest.id).source(doc);
            UpdateRequest updateRequest = new UpdateRequest(indexRequest.index(), this.docType, indexRequest.id())
                    .doc(doc)
                    .upsert(indexRequest);
            bulkRequest.add(updateRequest);
        }
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
