package org.example.mqtt.broker.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.util.ReferenceCounted;
import java.util.List;
import org.example.mqtt.model.ControlPacket;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public class MqttCodec extends ByteToMessageCodec<ControlPacket> {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ControlPacket cp) {
            // zero-copy
            ctx.write(cp.toByteBuf(), promise);
        }
        else {
            ctx.write(msg, promise);
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ControlPacket msg, ByteBuf out) {
        // code should not go here.
        throw new UnsupportedOperationException();
        // this will cause 1 time memory copy
        // out.writeBytes(msg.toByteBuf());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int packetLength = ControlPacket.tryPickupPacket(in);
        if (packetLength == -1) {// can not decode a packet
            return;
        }
        // in.readSlice can protect the in ByteBuf and check the packet protocol
        ControlPacket packet = ControlPacket.from(in.readSlice(packetLength));
        if (packet instanceof ReferenceCounted ref) {
            /**
             zero-copy. watch out: memory leak. this.payload use in inbound case will be release by netty.
             1. PublishInbound is a subclass of ReferenceCounted, so it will be released by the {@link io.netty.channel.DefaultChannelPipeline.TailContext.channelRead}
             */
            ref.retain();
        }
        out.add(packet);
    }

}
