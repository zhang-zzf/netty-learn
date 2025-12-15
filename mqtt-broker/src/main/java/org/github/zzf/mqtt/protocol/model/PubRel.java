package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class PubRel extends ControlPacket {

    private final short packetIdentifier;

    PubRel(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = incoming.readShort();
    }

    public PubRel(short packetIdentifier) {
        super((byte) 0x60, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        buf.writeShort(packetIdentifier);
        return buf;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":\"0x").append(Integer.toHexString(packetIdentifier & 0xffff)).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }

}
