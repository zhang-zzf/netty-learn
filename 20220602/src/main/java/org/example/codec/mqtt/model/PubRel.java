package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Accessors(chain = true)
public class PubRel extends ControlPacket {

    @Getter
    private short packetIdentifier;

    public PubRel(ByteBuf buf) {
        super(buf);
    }

    public PubRel(short packetIdentifier) {
        super((byte) 0x60, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = Unpooled.buffer(4);
        buf.writeByte(this._0byte);
        buf.writeByte(this.remainingLength);
        buf.writeShort(packetIdentifier);
        return buf;
    }

    @Override
    protected void initPacket() {
        this.packetIdentifier = this.buf.readShort();
    }

}
