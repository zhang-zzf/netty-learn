package org.example.mqtt.model;

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
        this.packetIdentifier = incoming.readShort();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packetIdentifier\":\"").append(pId()).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String pId() {
        return "0x" + Integer.toHexString(packetIdentifier & 0xffff);
    }

    public short packetIdentifier() {
        return packetIdentifier;
    }

}
