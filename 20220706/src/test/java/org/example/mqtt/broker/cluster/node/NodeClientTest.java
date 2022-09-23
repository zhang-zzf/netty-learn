package org.example.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

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

}