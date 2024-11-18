package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-13
 */
public class UnsubAck extends ControlPacket {

    private short packetIdentifier;

    UnsubAck(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = incoming.readShort();
    }

    public UnsubAck(short packetIdentifier) {
        super((byte) 0xB0, 0x02);
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
        sb.append("\"packetIdentifier\":").append(packetIdentifier).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

}
