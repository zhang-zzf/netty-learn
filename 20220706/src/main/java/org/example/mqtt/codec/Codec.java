package org.example.mqtt.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import org.example.mqtt.broker.ServerSessionHandler;
import org.example.mqtt.model.ControlPacket;

import java.util.List;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/22
 */
public class Codec extends ByteToMessageCodec<ControlPacket> {


    @Override
    protected void encode(ChannelHandlerContext ctx, ControlPacket msg, ByteBuf out) throws Exception {
        out.writeBytes(msg.toByteBuf());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int packetLength = ControlPacket.tryPickupPacket(in);
        if (packetLength == -1) {
            // can not decode a packet
            return;
        }
        // now we can decode a complete control packet.
        // use readRetainedSlice() to use zero-copy of ByteBuf (mostly in the direct area).
        // must release it after or it will cause memory leak
        ByteBuf packet = in.readRetainedSlice(packetLength);
        /**
         * The packet ByteBuf will be released by {@link ServerSessionHandler#channelRead(ChannelHandlerContext, Object)}
         */
        out.add(ControlPacket.from(packet));
    }

}
