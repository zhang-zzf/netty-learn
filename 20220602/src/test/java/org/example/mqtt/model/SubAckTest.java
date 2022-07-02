package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
class SubAckTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        List<Subscribe.Subscription> subscriptionList = new ArrayList<Subscribe.Subscription>() {{
            add(new Subscribe.Subscription("tbt/shanghai", 2));
            add(new Subscribe.Subscription("mqtt/shanghai", 0));
        }};
        SubAck out = SubAck.from(Short.MAX_VALUE, subscriptionList);
        then(out.subscriptionList()).hasSize(2);
        ByteBuf packet = out.toByteBuf();
        SubAck in = (SubAck) SubAck.from(packet);
        then(in.packetIdentifier()).isEqualTo(Short.MAX_VALUE);
    }

}