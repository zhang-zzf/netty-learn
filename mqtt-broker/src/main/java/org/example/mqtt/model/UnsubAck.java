package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-13
 */
public class UnsubAck extends ControlPacket {

    private short packetIdentifier;

    public UnsubAck(ByteBuf packet) {
        super(packet);
    }

    @Override
    protected void initPacket() {
        this.packetIdentifier = packet.readShort();
    }

    public static UnsubAck from(short packetIdentifier) {
        return new UnsubAck(packetIdentifier);
    }

    private UnsubAck(short packetIdentifier) {
        super((byte) 0xB0, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = fixedHeaderByteBuf();
        buf.writeShort(packetIdentifier);
        return buf;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packetIdentifier\":").append(packetIdentifier).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

}
