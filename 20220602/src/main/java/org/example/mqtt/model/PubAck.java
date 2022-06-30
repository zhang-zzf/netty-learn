package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PubAck extends ControlPacket {

    @Getter
    private short packetIdentifier;

    public PubAck(ByteBuf buf) {
        super(buf);
    }

    public PubAck(short packetIdentifier) {
        super((byte) 0x40, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = fixedHeaderByteBuf();
        buf.writeShort(packetIdentifier);
        return buf;
    }

    @Override
    protected void initPacket() {
        this.packetIdentifier = this.packet.readShort();
    }

}
