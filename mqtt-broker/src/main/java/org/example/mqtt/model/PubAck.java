package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PubAck extends ControlPacket {

    private short packetIdentifier;

    PubAck(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = incoming.readShort();
    }

    public PubAck(short packetIdentifier) {
        super((byte) 0x40, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        buf.writeShort(packetIdentifier);
        return buf;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":").append(pId()).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public short packetIdentifier() {
        return packetIdentifier;
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }

}
