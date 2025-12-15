package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
class PingRespTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        PingResp out = new PingResp();
        then(out.remainingLength).isEqualTo(0);
        ByteBuf packet = out.toByteBuf();
        PingResp in = (PingResp) PingResp.from(packet);
        then(in.remainingLength).isEqualTo(0);
    }

}