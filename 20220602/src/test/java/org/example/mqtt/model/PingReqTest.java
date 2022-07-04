package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
class PingReqTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        PingReq out = PingReq.from();
        then(out.remainingLength).isEqualTo(0);
        ByteBuf packet = out.toByteBuf();
        PingReq in = (PingReq) PingReq.from(packet);
        then(in.remainingLength).isEqualTo(0);
    }

}