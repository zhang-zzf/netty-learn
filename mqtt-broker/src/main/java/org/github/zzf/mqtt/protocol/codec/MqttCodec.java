package org.github.zzf.mqtt.protocol.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageCodec;
import java.util.List;
import org.github.zzf.mqtt.protocol.model.ControlPacket;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public class MqttCodec extends ByteToMessageCodec<ControlPacket> {

    @Override
    public void write(ChannelHandlerContext ctx,
            Object msg,
            ChannelPromise promise) {
        if (msg instanceof ControlPacket cp) {
            // core: zero-copy
            ByteBuf buf = cp.toByteBuf();
            // the owner of the buf will transfer to the netty
            // netty will release the buf after flush it to the wire
            ctx.write(buf, promise);
        }
        else {
            ctx.write(msg, promise);
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx,
            ControlPacket msg,
            ByteBuf out) {
        // code should not go here.
        throw new UnsupportedOperationException();
        // this will cause 1 time memory copy
        // out.writeBytes(msg.toByteBuf());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx,
            ByteBuf in,
            List<Object> out) {
        int packetLength = ControlPacket.tryPickupPacket(in);
        if (packetLength == -1) {// can not decode a packet
            return;
        }
        // core: zero-copy
        ByteBuf incoming = in.readRetainedSlice(packetLength);
        out.add(ControlPacket.from(incoming));
    }

}
