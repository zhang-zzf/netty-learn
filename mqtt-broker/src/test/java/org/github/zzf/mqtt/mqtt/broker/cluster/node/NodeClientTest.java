package org.github.zzf.mqtt.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
class NodeClientTest {

    @Test
    void givenByteArray_whenJSON_then() {
        NodeMessage m = new NodeMessage();
        m.setNodeId("nodeId");
        m.setPacket("Publish");
        String payload = "payload";
        m.setPayload(payload);
        String jsonStr = JSON.toJSONString(m);
        NodeMessage nm = JSON.parseObject(jsonStr, NodeMessage.class);
        then(nm.getPayload()).isEqualTo(payload);
    }

    @Test
    void givenCompleteFuture_whenException_then() throws ExecutionException, InterruptedException {
        CompletableFuture<Object> future = new CompletableFuture<>();
        future.whenComplete((r, e) -> {
            if (e != null) {
                log.error("unExpected exception in first whenComplete");
            }
        }).whenComplete((r, e) -> {
            if (e != null) {
                log.error("unExpected exception in second whenComplete");
            }
        });
        future.completeExceptionally(new TimeoutException());
    }

}