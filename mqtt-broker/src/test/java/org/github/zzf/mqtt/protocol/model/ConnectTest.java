package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

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


    @Test
    void givenPacket_whenToString_then() {
        Connect out = Connect.from("clientIdentifier", (short) 60);
        then(out.toString()).isNotNull();
    }

    /**
     * Will Connect
     */
    @Test
    void givenWill_whenBuildConnect_then() {
        Connect connect = Connect.from(2, false,
                (short) 64,
                "clientIdentifier",
                "will/1", Unpooled.buffer());
        ByteBuf buf = connect.toByteBuf();
        Connect packet = (Connect) Connect.from(buf);
        then(packet).returns(2, Connect::willQos)
                .returns(true, Connect::willFlag)
                .returns(false, Connect::willRetainFlag)
                .returns("will/1", Connect::willTopic)
        ;
    }

}