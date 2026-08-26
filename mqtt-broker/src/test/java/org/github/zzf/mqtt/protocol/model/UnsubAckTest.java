package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.github.zzf.mqtt.protocol.model.UnsubAck.BYTE_0;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
class UnsubAckTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        UnsubAck out = new UnsubAck(BYTE_0, 0x02, Short.MAX_VALUE);
        then(out.remainingLength).isEqualTo(2);
        ByteBuf packet = out.toByteBuf();
        UnsubAck in = (UnsubAck) UnsubAck.from(packet);
        then(in.remainingLength).isEqualTo(2);
    }

}