package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class Publish extends ControlPacket {

    public static final int AT_MOST_ONCE = 0;
    public static final int AT_LEAST_ONCE = 1;
    public static final int EXACTLY_ONCE = 2;
    public static final short UNKNOWN_PACKET_IDENTIFIER = 0;

    @Getter
    private String topicName;
    @Getter
    private short packetIdentifier;
    @Getter
    private ByteBuf payload;

    public Publish(ByteBuf receivedPacket) {
        super(receivedPacket);
    }

    public static Publish outgoing(Publish source, String topicName, Integer qos) {
        return Publish.outgoing(qos, UNKNOWN_PACKET_IDENTIFIER, source.payload, topicName);
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf header = fixedHeaderByteBuf();
        header.writeShort(topicName.length());
        header.writeCharSequence(topicName, UTF_8);
        if (needAck()) {
            header.writeShort(packetIdentifier);
        }
        // todo 待验证
        CompositeByteBuf packet = Unpooled.compositeBuffer()
                .addComponent(true, header)
                .addComponent(true, payload);
        // todo 待验证
        // important: release the payload ByteBuf
        this.payload.release(this.payload.refCnt());
        return packet;
    }

    /**
     * whether the qos need receiver ack
     *
     * @return true / false;
     */
    public static boolean needAck(int qos) {
        return qos == AT_LEAST_ONCE || qos == EXACTLY_ONCE;
    }

    /**
     * whether the packet need receiver ack
     *
     * @return true / false;
     */
    public boolean needAck() {
        return needAck(qos());
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
        int packetIdentifierLength = needAck(qos) ? 2 : 0;
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
        // important: use retainedSlice() to increase the refCnt
        this.payload = payload.retainedSlice();
    }

    private static byte build_0Byte() {
        return 0;
    }

    @Override
    protected void initPacket() {
        this.topicName = packet.readCharSequence(this.packet.readShort(), UTF_8).toString();
        if (needAck()) {
            this.packetIdentifier = packet.readShort();
        }
        // important: use realRetainedSlice to retain packet
        this.payload = packet.readRetainedSlice(packet.readableBytes());
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Publish publish = (Publish) o;
        return packetIdentifier == publish.packetIdentifier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetIdentifier);
    }

    public boolean atLeastOnce() {
        return qos() == AT_LEAST_ONCE;
    }

    public boolean exactlyOnce() {
        return qos() == EXACTLY_ONCE;
    }

    @Override
    public String toString() {
        return "Publish{" +
                "topicName='" + topicName + '\'' +
                ", packetIdentifier=" + packetIdentifier +
                ", payload=" + payload +
                ", _0byte=" + _0byte +
                '}';
    }

    public boolean atMostOnce() {
        return qos() == AT_MOST_ONCE;
    }

    /**
     * update packetIdentifier
     *
     * @param packetIdentifier the new id
     * @return this
     */
    public Publish packetIdentifier(short packetIdentifier) {
        this.packetIdentifier = packetIdentifier;
        return this;
    }

}
