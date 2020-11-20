package com.xiaohongshu.db.hercules.elasticsearchv6.schema.manager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.bulk.BulkItemResponse;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ElasticsearchManager {

    private static final Log LOG = LogFactory.getLog(ElasticsearchManager.class);

    protected RestHighLevelClient client;
    protected final String docType;

    public ElasticsearchManager(String endpoint, int port, String docType) {
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
    public void doUpsert(List<DocRequest> docRequests, int tryCount) {
        if (tryCount > 10) {
            throw new RuntimeException("send to elasticsearch fail with retry 10 times");
        }
        BulkRequest bulkRequest = new BulkRequest();
        for (DocRequest docRequest : docRequests) {
            Map<String, Object> doc = docRequest.getDoc();
            IndexRequest indexRequest = new IndexRequest().index(docRequest.index).type(this.docType).id(docRequest.id).source(doc);
            UpdateRequest updateRequest = new UpdateRequest().index(docRequest.index).type(this.docType).id(docRequest.id)
                    .doc(doc)
                    .upsert(indexRequest);
            bulkRequest.add(updateRequest);
        }
        BulkResponse response = doWithRetry(() -> silentBulk(bulkRequest), tryCount);
        handleError(docRequests, response, tryCount);
    }

    private BulkResponse doWithRetry(Supplier<BulkResponse> action, int tryCount) {
        for (int i = 0; i < tryCount; i++) {
            try {
                return action.get();
            } catch (Exception e) {
                try {
                    LOG.error("Exception found during action.get: "+e.toString());
                    Thread.sleep(1000 * i * 3);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return null;
    }

    BulkResponse silentBulk(BulkRequest bulkRequest) {
        try {
            return client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOG.error("Bulk error. bulk request:  " + bulkRequest.getDescription());
            throw new RuntimeException(e);
        }
    }

    private void handleError(List<DocRequest> docRequests, BulkResponse response, int tryCount) {
        if (response.hasFailures()) {
            LOG.error("error when bulk: " + response.buildFailureMessage());
            Set<String> rejectedDocIds = Arrays.stream(response.getItems())
                    .filter(BulkItemResponse::isFailed)
                    .filter(item -> item.getFailure() != null)
                    .filter(item -> item.getFailure().getCause() != null)
                    .peek(c -> LOG.error("error for: " + c.getId() + " reason:" + c.getFailure().getCause()))
                    .map(BulkItemResponse::getId)
                    .collect(Collectors.toSet());
            List<DocRequest> errorDocs = docRequests.stream()
                    .filter(doc -> rejectedDocIds.contains(doc.id))
                    .collect(Collectors.toList());
            doUpsert(errorDocs, tryCount + 1);
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
