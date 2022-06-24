package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@Accessors(chain = true)
public class PubRec extends ControlPacket {

    private short packetIdentifier;

    public PubRec(ByteBuf buf) {
        super(buf);
    }

    public PubRec(short packetIdentifier) {
        this.packetIdentifier = packetIdentifier;
        ByteBuf buf = Unpooled.buffer(4);
        buf.writeByte(0x50);
        buf.writeByte(0x02);
        buf.writeShort(packetIdentifier);
        this.setBuf(buf);
    }

    @Override
    protected void initPacket() {
        this.packetIdentifier = getBuf().readShort();
    }

}
