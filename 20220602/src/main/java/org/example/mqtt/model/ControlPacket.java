package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public abstract class ControlPacket {

    public static final int CONNECT = 1;
    public static final int SUBSCRIBE = 8;
    public static final int UNSUBSCRIBE = 10;
    public static final int DISCONNECT = 14;
    public static final int PUBCOMP = 7;
    public static final int PUBREL = 6;
    public static final int PUBREC = 5;
    public static final int PUBACK = 4;
    public static final int PUBLISH = 3;
    protected ByteBuf buf;
    protected byte _0byte;
    protected int remainingLength;

    protected ControlPacket(byte _0byte, int remainingLength) {
        this._0byte = _0byte;
        this.remainingLength = remainingLength;
    }

    /**
     * build incoming Packet
     *
     * @param packet packet
     */
    public ControlPacket(ByteBuf packet) {
        // todo : to be optimized
        this.buf = Unpooled.copiedBuffer(packet);
        this.buf.markReaderIndex();
        try {
            this._0byte = this.buf.readByte();
            this.remainingLength = readRemainingLength(this.buf);
            // should read all the bytes out of the packet.
            initPacket();
            if (this.buf.isReadable()) {
                // control packet is illegal.
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        } finally {
            this.buf.resetReaderIndex();
        }
    }

    /**
     * ByteBuf to model
     *
     * @param packet the data packet
     * @return model
     */
    public static ControlPacket from(ByteBuf packet) {
        ControlPacket controlPacket = convertToControlPacket(packet);
        if (!controlPacket.packetValidate()) {
            throw new IllegalArgumentException("packet validate failed: protocol violation.");
        }
        return controlPacket;
    }

    /**
     * convert ByteBuf to ControlPacket
     *
     * @param packet ByteBuf
     * @return ControlPacket
     */
    public static ControlPacket convertToControlPacket(ByteBuf packet) {
        byte _0byte = packet.getByte(packet.readerIndex());
        switch (type(_0byte)) {
            case CONNECT:
                return new Connect(packet);
            case PUBLISH:
                return new Publish(packet);
            case PUBACK:
                return new PubAck(packet);
            case PUBREC:
                return new PubRec(packet);
            case PUBREL:
                return new PubRel(packet);
            case PUBCOMP:
                return new PubComp(packet);
            case SUBSCRIBE:
                return new Subscribe(packet);
            case UNSUBSCRIBE:
                return new Unsubscribe(packet);
            case 12:
                return new PingReq(packet);
            case DISCONNECT:
                return new Disconnect(packet);
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * validate the packet after build it
     */
    protected boolean packetValidate() {
        return true;
    }

    /**
     * build incoming Packet
     */
    protected void initPacket() {
        // none
    }

    public static byte type(byte _0byte) {
        return (byte) (_0byte >> 4);
    }

    public byte type() {
        return type(this._0byte);
    }

    private int readRemainingLength(ByteBuf buf) {
        int remainingLength = 0;
        int multiplier = 1;
        while (true) {
            byte encodeByte = buf.readByte();
            remainingLength += (encodeByte & 0x7F) * multiplier;
            if ((encodeByte & 0x80) == 0) {
                break;
            }
            multiplier *= 0x80;
            if (multiplier > 0x80 * 0x80 * 0x80) {
                throw new IllegalArgumentException();
            }
        }
        return remainingLength;
    }

    /**
     * model to ByteBuf
     *
     * @return ByteBuf
     */
    public ByteBuf toByteBuf() {
        throw new UnsupportedOperationException();
    }

    protected ByteBuf fixedHeaderByteBuf() {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeByte(this._0byte);
        // remainingLength field
        int remainingLength = this.remainingLength;
        do {
            int encodedByte = remainingLength % 128;
            remainingLength /= 128;
            if (remainingLength > 0) {
                encodedByte = (encodedByte | 128);
            }
            buf.writeByte(encodedByte);
        } while (remainingLength > 0);
        return buf;
    }

}
