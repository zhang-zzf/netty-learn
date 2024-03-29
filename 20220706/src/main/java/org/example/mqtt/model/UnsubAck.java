package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class UnsubAck extends ControlPacket {

    private short packetIdentifier;

    public UnsubAck(ByteBuf packet) {
        super(packet);
    }

    @Override
    protected void initPacket() {
        this.packetIdentifier = content().readShort();
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
