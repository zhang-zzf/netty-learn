package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
class SubAckTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        List<Integer> reasonCodes = new ArrayList<>() {{
            add(0);
            add(2);
        }};
        SubAck out = SubAck.from(Short.MAX_VALUE, reasonCodes);
        ByteBuf packet = out.toByteBuf();
        SubAck in = (SubAck) SubAck.from(packet);
        then(in.packetIdentifier()).isEqualTo(Short.MAX_VALUE);
    }

}