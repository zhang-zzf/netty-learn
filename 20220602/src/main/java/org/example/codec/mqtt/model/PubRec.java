package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PubRec extends ControlPacket {

    private short packetIdentifier;

    public PubRec(ByteBuf buf) {
        super(buf);
    }

    public PubRec(short packetIdentifier) {
        super((byte) 0x50, 0x02);
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
        this.packetIdentifier = this.buf.readShort();
    }

}
