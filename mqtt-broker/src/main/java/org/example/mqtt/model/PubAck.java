package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PubAck extends ControlPacket {

    private short packetIdentifier;

    public PubAck(ByteBuf buf) {
        super(buf);
    }

    public static PubAck from(short packetIdentifier) {
        return new PubAck(packetIdentifier);
    }

    private PubAck(short packetIdentifier) {
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
        this.packetIdentifier = packet.readShort();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packetIdentifier\":\"0x").append(Integer.toHexString(packetIdentifier & 0xffff)).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public short packetIdentifier() {
        return packetIdentifier;
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }

}
