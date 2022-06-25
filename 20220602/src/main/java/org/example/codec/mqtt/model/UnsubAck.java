package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class UnsubAck extends ControlPacket {

    @Getter
    private final short packetIdentifier;

    public UnsubAck(short packetIdentifier) {
        super((byte) 0xB0, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = fixedHeaderByteBuf();
        buf.writeShort(packetIdentifier);
        return buf;
    }

}
