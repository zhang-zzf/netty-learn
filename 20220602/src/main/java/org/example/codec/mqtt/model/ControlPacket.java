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
        return (byte) (_0Byte() & 0xF0);
    }

    protected byte _0Byte() {
        return buf.getByte(0);
    }

    private int fixedHeaderByteCnt() {
        return 1 + remainingLengthByteCnt;
    }

}
