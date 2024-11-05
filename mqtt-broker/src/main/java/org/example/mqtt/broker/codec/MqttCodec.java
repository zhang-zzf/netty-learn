package org.example.mqtt.broker.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import org.example.mqtt.model.ControlPacket;

import java.util.List;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/22
 */
public class MqttCodec extends ByteToMessageCodec<ControlPacket> {

    @Override
    protected void encode(ChannelHandlerContext ctx, ControlPacket msg, ByteBuf out) {
        out.writeBytes(msg.toByteBuf());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int packetLength = ControlPacket.tryPickupPacket(in);
        if (packetLength == -1) {// can not decode a packet
            return;
        }
        // use readRetainedSlice() to use zero-copy of ByteBuf (mostly in the direct area).
        // the retained ByteBuf must be released after the business deal with the ControlPacket,
        // or it will cause memory leak
        out.add(ControlPacket.from(in.readRetainedSlice(packetLength)));
    }

}
