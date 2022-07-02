package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

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

    private String topicName;
    private short packetIdentifier;
    private ByteBuf payload;

    /**
     * inbound packet convert to model
     *
     * @param receivedPacket inbound packet
     */
    public Publish(ByteBuf receivedPacket) {
        super(receivedPacket);
    }

    /**
     * Publish to Publish use Zero-Copy of ByteBuf for payload
     *
     * @param origin source
     * @return a Publish Packet that have the save data as source
     */
    public static Publish retained(Publish origin) {
        ByteBuf retainedPayload = origin.payload.retainedSlice();
        return new Publish(origin._0byte, origin.remainingLength,
                origin.packetIdentifier,
                retainedPayload, origin.topicName);
    }

    public static Publish outgoing(boolean retain, byte qos, boolean dup,
                                   String topicName, short packetIdentifier, ByteBuf payload) {
        byte _0byte = build_0Byte(retain, qos, dup);
        int topicLength = topicName.length() + 2;
        // remainingLength field
        int packetIdentifierLength = needAck(qos) ? 2 : 0;
        int remainingLength = topicLength + packetIdentifierLength + payload.readableBytes();
        return new Publish(_0byte, remainingLength, packetIdentifier, payload, topicName);
    }


    @Override
    public ByteBuf toByteBuf() {
        ByteBuf header = fixedHeaderByteBuf();
        byte[] topicNameBytes = topicName.getBytes(UTF_8);
        header.writeShort(topicNameBytes.length);
        header.writeBytes(topicNameBytes);
        if (needAck()) {
            header.writeShort(packetIdentifier);
        }
        return Unpooled.compositeBuffer()
                .addComponent(true, header)
                .addComponent(true, this.payload);
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
     * outgoing Publish Message
     */
    private Publish(byte _0byte, int remainingLength, short packetIdentifier, ByteBuf payload, String topicName) {
        super(_0byte, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.topicName = topicName;
        // important: use retainedSlice() to increase the refCnt
        this.payload = payload.retainedSlice();
    }

    static byte build_0Byte(boolean retain, byte qos, boolean dup) {
        byte _0Byte = 0x30;
        if (retain) {
            _0Byte |= 0x01;
        }
        _0Byte |= qos << 1;
        if (dup) {
            _0Byte |= 0x08;
        }
        return _0Byte;
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
        if (qos() == 0 && dup()) {
            return false;
        }
        if ((qos() & 0x03) == 0x03) {
            return false;
        }
        return super.packetValidate();
    }

    public boolean dup() {
        return (_0byte & 0x08) != 0;
    }

    public int qos() {
        return (this._0byte & 0x06) >> 1;
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

    public ByteBuf payload() {
        return this.payload;
    }

    public String topicName() {
        return this.topicName;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    public Publish topicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public Publish qos(byte qos) {
        this.updateQos(qos);
        return this;
    }

    private void updateQos(byte qos) {
        int clearQoS = this._0byte & 0xF9;
        this._0byte = (byte) (clearQoS & qos);
    }

}
