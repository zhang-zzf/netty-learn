package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PubRec extends ControlPacket {

    @Getter
    private short packetIdentifier;

    public PubRec(ByteBuf buf) {
        super(buf);
    }

    private PubRec(short packetIdentifier) {
        super((byte) 0x50, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    public static PubRec from(short packetIdentifier) {
        return new PubRec(packetIdentifier);
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = fixedHeaderByteBuf();
        buf.writeShort(packetIdentifier);
        return buf;
    }

    @Override
    protected void initPacket() {
        this.packetIdentifier = _buf().readShort();
    }

}
