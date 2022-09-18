package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.experimental.Accessors;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Accessors(chain = true)
public class PubRel extends ControlPacket {

    private short packetIdentifier;

    public PubRel(ByteBuf buf) {
        super(buf);
    }

    private PubRel(short packetIdentifier) {
        super((byte) 0x60, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    public static PubRel from(short packetIdentifier) {
        return new PubRel(packetIdentifier);
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = Unpooled.buffer(4);
        buf.writeByte(this.byte0);
        buf.writeByte(this.remainingLength);
        buf.writeShort(packetIdentifier);
        return buf;
    }

    @Override
    protected void initPacket() {
        this.packetIdentifier = content().readShort();
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packetIdentifier\":\"0x").append(Integer.toHexString(packetIdentifier & 0xffff)).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }

}
