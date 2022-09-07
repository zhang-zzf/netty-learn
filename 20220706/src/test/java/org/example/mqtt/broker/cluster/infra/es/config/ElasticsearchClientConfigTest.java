package org.example.mqtt.broker.cluster.infra.es.config;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.ResponseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

@Slf4j
class ElasticsearchClientConfigTest {

    static ElasticsearchClient client;
    static ElasticsearchAsyncClient asyncclient;

    @BeforeAll
    public static void beforeAll() {
        String host = "nudocker01";
        int inetPort = 9120;
        ElasticsearchClientConfig config = new ElasticsearchClientConfig();
        client = config.elasticsearchClient(host, inetPort);
        asyncclient = config.elasticsearchAsyncClient(host, inetPort);
    }

    @Test
    void givenIndices_whenAsyncCreateIndex_then() throws IOException, ExecutionException, InterruptedException {
        String indexName = UUID.randomUUID().toString();
        BiConsumer<Object, Throwable> responseHandler = (resp, throwable) -> {
            if (throwable != null) {
                log.error("Exception.", throwable);
            } else {
                log.info("resp: {}", resp);
            }
        };
        asyncclient.indices()
                .create(new CreateIndexRequest.Builder().index(indexName).build())
                .whenComplete(responseHandler)
                // just for test
                .get()
        ;
        asyncclient.indices()
                .delete(new DeleteIndexRequest.Builder().index(indexName).build())
                .whenComplete(responseHandler)
                // just for test
                .get()
        ;
        asyncclient.indices()
                .create(index -> index.index(indexName))
                .whenComplete(responseHandler)
                // just for test
                .get()
        ;
        asyncclient.indices()
                .delete(req -> req.index(indexName))
                .whenComplete(responseHandler)
                // just for test
                .get();
        ;

    }

    @Test
    void givenIndices_whenCreateIndex_then() throws IOException {
        String indexName = UUID.randomUUID().toString();
        client.indices()
                .create(new CreateIndexRequest.Builder().index(indexName).build());
        client.indices()
                .delete(new DeleteIndexRequest.Builder().index(indexName).build());
        client.indices()
                .create(index -> index.index(indexName));
        client.indices()
                .delete(req -> req.index(indexName));
    }


    /**
     * createIfNotExist(put if absent)
     * <p>不存在创建</p>
     */
    @Test
    void given_whenPutIfAbsent_then() throws IOException {
        String index = UUID.randomUUID().toString();
        IndexResponse resp1 = client.index(req -> req.index(index)
                .id(index)
                .document(new HashMap<>())
                .opType(OpType.Create)
        );
        log.info("resp1: {}", resp1);
        // index with the same id
        try {
            IndexResponse resp2 = client.index(req -> req.index(index)
                    .id(index)
                    .document(new HashMap<>())
                    .opType(OpType.Create)
            );
            log.info("resp2: {}", resp2);
        } catch (ResponseException e) {
            log.info("e: {}", e);
        }
        client.indices().delete(req -> req.index(index));
    }

    /**
     * deleteNotExist
     * <p>删除不存在的文档</p>
     */
    @Test
    void given_whenDeleteNotExist_then() throws IOException, InterruptedException {
        String index = UUID.randomUUID().toString();
        IndexResponse resp1 = client.index(req -> req.index(index)
                .id(index)
                .document(new HashMap<>())
                .opType(OpType.Create)
        );
        log.info("resp1: {}", resp1);
        // index with the same id
        DeleteResponse resp2 = client.delete(req -> req.index(index).id(index));
        log.info("resp2: {}", resp2);
        DeleteResponse resp3 = client.delete(req -> req.index(index).id(index));
        log.info("resp3: {}", resp3);
        if ("NotFound".equals(resp3.result().name())) {
            log.info("deleteNotExist document");
        }
        client.indices().delete(req -> req.index(index));
    }

}