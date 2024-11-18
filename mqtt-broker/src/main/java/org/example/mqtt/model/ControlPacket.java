package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-17
 */
@Slf4j
public abstract class ControlPacket {

    public static final int _0_BYTE_LENGTH = 1;
    public static final int MIN_PACKET_LENGTH = 2;

    public static final byte CONNECT = 0x10;
    public static final byte CONNACK = 0x20;
    public static final byte PUBLISH = 0x30;
    public static final byte PUBACK = 0x40;
    public static final byte PUBREC = 0x50;
    public static final byte PUBREL = 0x60;
    public static final byte PUBCOMP = 0x70;
    public static final byte SUBSCRIBE = (byte) 0x80;
    public static final byte SUBACK = (byte) 0x90;
    public static final byte UNSUBSCRIBE = (byte) 0xA0;
    public static final byte UNSUBACK = (byte) 0xB0;
    public static final byte PINGREQ = (byte) 0xC0;
    public static final byte PINGRESP = (byte) 0xD0;
    public static final byte DISCONNECT = (byte) 0xE0;

    protected byte byte0;
    protected int remainingLength;

    protected ControlPacket(byte byte0, int remainingLength) {
        this.byte0 = byte0;
        this.remainingLength = remainingLength;
    }

    /**
     * build incoming Packet
     *
     * @param incoming packet
     */
    protected ControlPacket(ByteBuf incoming) {
        this.byte0 = incoming.readByte();
        this.remainingLength = readRemainingLength(incoming);
    }

    /**
     * ByteBuf to model
     *
     * @param incoming the data packet
     * @return model
     */
    public static ControlPacket from(ByteBuf incoming) {
        incoming.markReaderIndex();
        try {
            ControlPacket controlPacket = buildControlPacketFrom(incoming);
            // should read all the bytes out of the packet.
            if (incoming.isReadable()) {
                // control packet is illegal.
                throw new IllegalArgumentException();
            }
            if (!controlPacket.packetValidate()) {
                log.error("ControlPacket validate failed->{}", controlPacket);
                throw new IllegalArgumentException("packet validate failed: protocol violation.");
            }
            return controlPacket;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        } finally {
            incoming.resetReaderIndex();
        }
    }

    /**
     * convert ByteBuf to ControlPacket
     *
     * @param incoming ByteBuf
     * @return ControlPacket
     */
    private static ControlPacket buildControlPacketFrom(ByteBuf incoming) {
        byte _0byte = incoming.getByte(incoming.readerIndex());
        switch (type(_0byte)) {
            case CONNECT:
                return new Connect(incoming);
            case CONNACK:
                return new ConnAck(incoming);
            case PUBLISH:
                // core
                return new PublishInbound(incoming);
            case PUBACK:
                return new PubAck(incoming);
            case PUBREC:
                return new PubRec(incoming);
            case PUBREL:
                return new PubRel(incoming);
            case PUBCOMP:
                return new PubComp(incoming);
            case SUBSCRIBE:
                return new Subscribe(incoming);
            case SUBACK:
                return new SubAck(incoming);
            case UNSUBSCRIBE:
                return new Unsubscribe(incoming);
            case UNSUBACK:
                return new UnsubAck(incoming);
            case PINGREQ:
                return new PingReq(incoming);
            case PINGRESP:
                return new PingResp(incoming);
            case DISCONNECT:
                return new Disconnect(incoming);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static int tryPickupPacket(ByteBuf in) {
        int packetLength = -1;
        if (in.readableBytes() < MIN_PACKET_LENGTH) {
            return packetLength;
        }
        in.markReaderIndex();
        try {
            in.readByte();
            int remainingLength = readRemainingLength(in);
            if (in.readableBytes() < remainingLength) {
                return packetLength;
            }
            // fixed header length + remainingLength
            packetLength = (_0_BYTE_LENGTH + remainingLengthByteCnt(remainingLength)) + remainingLength;
        } catch (Exception e) {
            in.resetReaderIndex();
            log.error("tryPickupPacket failed: {}", ByteBufUtil.hexDump(in));
            throw e;
        } finally {
            in.resetReaderIndex();
        }
        return packetLength;
    }

    private static int remainingLengthByteCnt(int remainingLength) {
        return remainingLengthToByteBuf(remainingLength).readableBytes();
    }

    /**
     * validate the packet after build it
     */
    protected boolean packetValidate() {
        return true;
    }

    public static byte type(byte _0byte) {
        return (byte) (_0byte & 0xF0);
    }

    public byte type() {
        return type(this.byte0);
    }

    private static int readRemainingLength(ByteBuf buf) {
        int rl = 0;
        int multiplier = 1;
        while (true) {
            if (buf.readableBytes() == 0) {
                // remainLength is 4 bytes, but now just received 2 bytes
                return Integer.MAX_VALUE;
            }
            byte encodeByte = buf.readByte();
            rl += (encodeByte & 0x7F) * multiplier;
            if ((encodeByte & 0x80) == 0) {
                break;
            }
            multiplier *= 0x80;
            if (multiplier > 0x80 * 0x80 * 0x80) {
                throw new IllegalArgumentException();
            }
        }
        return rl;
    }

    /**
     * model to ByteBuf
     *
     * @return ByteBuf
     */
    public abstract ByteBuf toByteBuf();

    protected ByteBuf fixedHeaderByteBuf() {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeByte(this.byte0);
        // remainingLength field
        buf.writeBytes(remainingLengthToByteBuf(this.remainingLength));
        return buf;
    }

    private static ByteBuf remainingLengthToByteBuf(int remainingLength) {
        ByteBuf buf = Unpooled.buffer(4);
        int rl = remainingLength;
        do {
            int encodedByte = rl % 128;
            rl /= 128;
            if (rl > 0) {
                encodedByte = (encodedByte | 128);
            }
            buf.writeByte(encodedByte);
        } while (rl > 0);
        return buf;
    }

    public static String hexPId(short packetIdentifier) {
        return "0x" + Integer.toHexString(packetIdentifier & 0xffff);
    }

    public static Short hexPIdToShort(String hexPId) {
        return Integer.valueOf(hexPId.substring(2), 16).shortValue();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"byte0\":").append(byte0).append(',');
        sb.append("\"remainingLength\":").append(remainingLength).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }
}

