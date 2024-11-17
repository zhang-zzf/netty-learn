package org.example.mqtt.broker.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import org.example.mqtt.model.ControlPacket;

import java.util.List;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
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
        out.add(ControlPacket.from(in.readSlice(packetLength)));
    }

}
