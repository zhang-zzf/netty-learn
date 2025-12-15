package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class PubAck extends ControlPacket {

    private final short packetIdentifier;

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
