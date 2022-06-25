package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public abstract class ControlPacket {

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
     * @param buf packet
     */
    public ControlPacket(ByteBuf buf) {
        this.buf = Unpooled.copiedBuffer(buf);
        buf.markReaderIndex();
        try {
            this._0byte = buf.readByte();
            this.remainingLength = readRemainingLength(buf);
            // should read all the bytes out of the packet.
            initPacket();
            if (this.buf.isReadable()) {
                // control packet is illegal.
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException();
        } finally {
            buf.resetReaderIndex();
        }
    }

    /**
     * ByteBuf to model
     *
     * @param packet the data packet
     * @return model
     */
    public static ControlPacket from(ByteBuf packet) {
        byte _0byte = packet.getByte(packet.readerIndex());
        switch (type(_0byte)) {
            case 1:
                return new Connect(packet);
            case 3:
                return new Publish(packet);
            case 4:
                return new PubAck(packet);
            case 5:
                return new PubRec(packet);
            case 6:
                return new PubRel(packet);
            case 7:
                return new PubComp(packet);
            case 8:
                return new Subscribe(packet);
            case 10:
                return new Unsubscribe(packet);
            case 12:
                return new PingReq(packet);
            case 14:
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
        return (byte) (_0byte & 0xF0);
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
