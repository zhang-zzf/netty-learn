package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.BDDAssertions.then;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.hexPId;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.hexPIdToShort;

@Slf4j
class ControlPacketTest {

    @Test
    void given_whenHexAndDeHex_then() {
        AtomicLong cnt = new AtomicLong(0);
        for (short i = Short.MIN_VALUE; ; i++) {
            then(hexPIdToShort(hexPId(i))).isEqualTo(i);
            cnt.getAndIncrement();
            if (i == Short.MAX_VALUE) {
                break;
            }
        }
        then(cnt.get()).isEqualTo(65536L);
    }

    @Test
    void givenPartTcpPacket_whenDecodeMqttControlPacket_then() {
        // 0x30f2 -> Publish Packet but just part ot it.
        ByteBuf buf = Unpooled.buffer(2);
        buf.writeByte(0x30);
        buf.writeByte(0xf2);
        // can not decode a ControlPacket
        then(ControlPacket.tryPickupPacket(buf)).isEqualTo(-1);
    }

}