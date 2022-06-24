package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@NoArgsConstructor
public abstract class ControlPacket {

    protected ByteBuf buf;
    private int remainingLengthByteCnt;

    public ControlPacket(ByteBuf buf) {
        this.buf = Unpooled.copiedBuffer(buf);
        this.remainingLengthByteCnt = decideByteCnt(this.buf);
        buf.markReaderIndex();
        try {
            buf.skipBytes(fixedHeaderByteCnt());
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

    public static ControlPacket fromPocket(ByteBuf packet) {
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

    public ByteBuf getBuf() {
        // validate packet
        packetValidate();
        return this.buf;
    }

    public boolean packetValidate() {
        return true;
    }

    public ControlPacket setBuf(ByteBuf buf) {
        this.buf = buf.discardReadBytes();
        return this;
    }

    protected void initPacket() {
        // none
    }

    private int decideByteCnt(ByteBuf buf) {
        int cnt = 1;
        while (true) {
            byte encodeByte = buf.readByte();
            if ((encodeByte & 0x80) == 0) {
                break;
            }
            cnt += 1;
        }
        if (cnt > 4) {
            throw new IllegalArgumentException();
        }
        return cnt;
    }

    /**
     * MQTT Control Packet type
     */
    public byte type() {
        return type(_0Byte());
    }

    public static byte type(byte _0byte) {
        return (byte) (_0byte & 0xF0);
    }

    protected byte _0Byte() {
        return buf.getByte(0);
    }

    private int fixedHeaderByteCnt() {
        return 1 + remainingLengthByteCnt;
    }

}
