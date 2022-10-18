package org.example.mqtt.broker.cluster.node;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class NodeMessageTest {


    @Test
    void givenNodeMessage_whenBrokerClose_then() {
        NodeMessage nm = NodeMessage.wrapBrokerClose("zzf.zhang", "mqtt");
        log.info("BrokerClose: {}", nm);
    }


    @Test
    void givenNodeMessage_whenWrapPublish_then() {

    }
}