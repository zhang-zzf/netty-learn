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
        ByteBuf buf = getBuf();
        this.topicName = buf.readCharSequence(this.buf.readShort(), UTF_8).toString();
        if (qos() > 0) {
            this.packetIdentifier = buf.readShort();
        }
        this.payload = buf.readSlice(buf.readableBytes());
    }

    @Override
    public boolean packetValidate() {
        if ((_0Byte() & 0x01) != 0) {
            return false;
        }
        // The DUP flag MUST be set to 0 for all QoS 0 messages
        if (qos() == 0 && dupFlag()) {
            return false;
        }
        if ((qos() & 0x03) == 0x03) {
            return false;
        }
        return super.packetValidate();
    }

    public boolean dupFlag() {
        return (_0Byte() & 0x08) != 0;
    }

    public int qos() {
        return _0Byte() & 0x06;
    }

    public boolean retain() {
        return (_0Byte() & 0x01) != 0;
    }


}
