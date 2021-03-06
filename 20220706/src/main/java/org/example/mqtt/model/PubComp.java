package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PubComp extends ControlPacket {

    @Getter
    private short packetIdentifier;

    public PubComp(ByteBuf buf) {
        super(buf);
    }

    private PubComp(short packetIdentifier) {
        super((byte) 0x70, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    public static PubComp from(short packetIdentifier) {
        return new PubComp(packetIdentifier);
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packetIdentifier\":").append(packetIdentifier).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
