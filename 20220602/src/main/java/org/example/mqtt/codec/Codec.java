package org.example.mqtt.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import org.example.mqtt.model.ControlPacket;

import java.util.List;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/22
 */
public class Codec extends ByteToMessageCodec<ControlPacket> {

    public static final int _0_BYTE_LENGTH = 1;
    public static final int MIN_PACKET_LENGTH = 2;

    @Override
    protected void encode(ChannelHandlerContext ctx, ControlPacket msg, ByteBuf out) throws Exception {
        out.writeBytes(msg.toByteBuf());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < MIN_PACKET_LENGTH) {
            return;
        }
        int[] remainingLength = queryRemainingLength(in);
        // not enough bytes for remainingLength field.
        if (remainingLength[0] == -1) {
            return;
        }
        // fixed header length + remainingLength
        int packetLength = (_0_BYTE_LENGTH + remainingLength[1]) + remainingLength[0];
        // not enough bytes for a packet
        if (in.readableBytes() < packetLength) {
            return;
        }
        // now we can decode a complete control packet.
        // use readRetainedSlice() to use zero-copy of ByteBuf (mostly in the direct area).
        // must release it after or it will cause memory leak
        ByteBuf packet = in.readRetainedSlice(packetLength);
        /**
         * The packet ByteBuf will be released by {@link SessionHandler#channelRead(ChannelHandlerContext, Object)}
         */
        out.add(ControlPacket.from(packet));
    }

    private int[] queryRemainingLength(ByteBuf in) {
        int nextReaderIndex = in.readerIndex() + 1;
        int remainingLength = 0;
        int remainingLengthByteCnt = 1;
        int multiplier = 1;
        while (true) {
            byte encodeByte = in.getByte(nextReaderIndex++);
            remainingLength += (encodeByte & 0x7F) * multiplier;
            if ((encodeByte & 0x80) == 0) {
                break;
            }
            // need next byte but the buf does not have it
            if (nextReaderIndex >= in.writerIndex()) {
                return new int[]{-1, -1};
            }
            remainingLengthByteCnt += 1;
            multiplier *= 0x80;
            if (multiplier > 0x80 * 0x80 * 0x80) {
                throw new IllegalArgumentException();
            }
        }
        return new int[]{remainingLength, remainingLengthByteCnt};
    }

}
