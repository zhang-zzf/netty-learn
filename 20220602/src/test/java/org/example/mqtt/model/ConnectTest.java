package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static io.netty.buffer.Unpooled.buffer;
import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/29
 */
class ConnectTest {

    /**
     * Connect 正常流程测试
     */
    @Test
    void givenRightByteBufPacket_whenCreate_thenSuccess() {
        // build outgoing packet
        Connect connect = Connect.from("clientIdentifier", (short) 60);
        then(connect.packetValidate()).isTrue();
        // user outgoing packet as ingoing packet
        ByteBuf buf = connect.toByteBuf();
        ControlPacket packet = Connect.from(buf);
        then(packet.packetValidate()).isTrue();
    }

}