package org.github.zzf.mqtt.mqtt.broker.cluster.node;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
class NodeMessageTest {


    @Test
    void givenNodeMessage_whenBrokerClose_then() {
        NodeMessage nm = NodeMessage.wrapBrokerClose("zzf.zhang", "mqtt");
        log.info("BrokerClose: {}", nm);
    }


    @Test
    void givenNodeMessage_whenWrapPublish_then() {
        NodeMessage nodeMessage = NodeMessage.wrapConnect("node1", "cId1");
        ByteBuf buf = nodeMessage.toByteBuf();
        byte aByte = buf.getByte(0);
        then(aByte).isEqualTo((byte) '{');
    }
}