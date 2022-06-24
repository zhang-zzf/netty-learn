package org.example.codec.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.example.codec.mqtt.model.FixedHeader;

import java.util.List;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/22
 */
public class Decoder extends ByteToMessageDecoder {

    public static final int CONNECT = 1;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        // read the first byte;
        final byte byte_1 = in.readByte();
        long remainingLength = queryRemainingLength(in);
        // not enough bytes for a control packet
        if (remainingLength == -1 || in.readableBytes() < remainingLength) {
            in.resetReaderIndex();
            return;
        }
        final FixedHeader fixedHeader = new FixedHeader(byte_1, remainingLength);
        // now we can decode a complete control packet.
        switch (decidePacketType(byte_1)) {
            case CONNECT:
                break;
            default:
                throw new IllegalArgumentException("packet type is illegal: " + decidePacketType(byte_1));
        }

    }

    private int decidePacketType(byte byte_1) {
        return byte_1 & 0xf0;
    }

    private long queryRemainingLength(ByteBuf in) {
        long remainingLength = 0;
        int multiplier = 1;
        while (in.isReadable()) {
            byte encodeByte = in.readByte();
            remainingLength += (encodeByte & 0x7F) * multiplier;
            if ((encodeByte & 0x80) == 0) {
                break;
            }
            if (!in.isReadable()) {
                remainingLength = -1;
                break;
            }
            multiplier *= 0x80;
            if (multiplier > 0x80 * 0x80 * 0x80) {
                throw new IllegalArgumentException();
            }
        }
        return remainingLength;
    }

}
