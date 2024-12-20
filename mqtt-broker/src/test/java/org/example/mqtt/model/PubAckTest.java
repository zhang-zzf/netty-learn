package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
class PubAckTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        PubAck out = new PubAck(Short.MAX_VALUE);
        then(out.remainingLength).isEqualTo(2);
        ByteBuf packet = out.toByteBuf();
        PubAck in = (PubAck) PubAck.from(packet);
        then(in.remainingLength).isEqualTo(2);
    }

}