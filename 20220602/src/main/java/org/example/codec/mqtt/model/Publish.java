package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.experimental.Accessors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@Accessors(chain = true)
public class Publish extends ControlPacket {

    private String topicName;
    private short packetIdentifier;
    private ByteBuf payload;

    public Publish(ByteBuf receivedPacket) {
        super(receivedPacket);
    }

    @Override
    protected void initPacket() {
        this.topicName = getBuf().readCharSequence(buf.readShort(), UTF_8).toString();
        if (qos() > 0) {
            this.packetIdentifier = getBuf().readShort();
        }
        this.payload = getBuf().readSlice(getBuf().readableBytes());
    }

    public int dupFlag() {
        return _0Byte() & 0x08;
    }

    public int qos() {
        byte byte_1 = buf.getByte(0);
        return _0Byte() & 0x06;
    }

    public boolean retain() {
        return (_0Byte() & 0x01) != 0;
    }


}
