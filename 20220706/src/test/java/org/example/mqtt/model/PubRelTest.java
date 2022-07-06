package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
class PubRelTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        PubRel out = PubRel.from(Short.MAX_VALUE);
        then(out.remainingLength).isEqualTo(2);
        ByteBuf packet = out.toByteBuf();
        PubRel in = (PubRel) PubRel.from(packet);
        then(in.remainingLength).isEqualTo(2);
    }

}