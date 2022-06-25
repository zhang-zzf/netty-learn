package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class Publish extends ControlPacket {

    @Getter
    private String topicName;
    @Getter
    private short packetIdentifier;
    @Getter
    private ByteBuf payload;

    public Publish(ByteBuf receivedPacket) {
        super(receivedPacket);
    }

    @Override
    public ByteBuf toByteBuf() {
        // todo direct buffer?
        ByteBuf header = fixedHeaderByteBuf();
        header.writeShort(topicName.length());
        header.writeCharSequence(topicName, UTF_8);
        if (qos() > 0) {
            header.writeShort(packetIdentifier);
        }
        return Unpooled.compositeBuffer().addComponent(header).addComponent(payload);
    }

    /**
     * 构建 outgoing Publish Message
     */
    public static Publish outgoing(int qos, short packetIdentifier, ByteBuf payload, String topicName) {
        if (topicName.length() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("TopicName is Illegal: overflow");
        }
        byte _0byte = build_0Byte();
        int topicLength = topicName.length() + 2;
        // remainingLength field
        int packetIdentifierLength = qos > 0 ? 2 : 0;
        int remainingLength = topicLength + packetIdentifierLength + payload.readableBytes();
        return new Publish(_0byte, remainingLength, packetIdentifier, payload, topicName);
    }


    /**
     * 构建 outgoing Publish Message
     */
    protected Publish(byte _0byte, int remainingLength, short packetIdentifier, ByteBuf payload, String topicName) {
        super(_0byte, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.topicName = topicName;
        this.payload = payload;
    }

    private static byte build_0Byte() {
        return 0;
    }

    @Override
    protected void initPacket() {
        this.topicName = buf.readCharSequence(this.buf.readShort(), UTF_8).toString();
        if (qos() > 0) {
            this.packetIdentifier = buf.readShort();
        }
        this.payload = buf.readSlice(buf.readableBytes());
    }

    @Override
    protected boolean packetValidate() {
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
        return (_0byte & 0x08) != 0;
    }

    public int qos() {
        return this._0byte & 0x06;
    }

    public boolean retain() {
        return (_0byte & 0x01) != 0;
    }

}
