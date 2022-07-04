package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
class ConnAckTest {

    /**
     * ConnAck 正常流程测试
     */
    @Test
    void givenPacket_whenOutAndIn_thenSuccess() {
        // build outgoing packet
        ConnAck out = ConnAck.accepted();
        then(out.sp()).isFalse();
        // user outgoing packet as ingoing packet
        ByteBuf buf = out.toByteBuf();
        ConnAck in = (ConnAck) ConnAck.from(buf);
        then(in.returnCode()).isEqualTo(0x00);
    }

}