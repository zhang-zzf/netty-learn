package org.github.zzf.mqtt.protocol.codec;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.INCOMPLETE_PACKET;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.DecoderException;
import io.netty.util.ReferenceCountUtil;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50;
import org.github.zzf.mqtt.protocol.model.Publish;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
@Slf4j
public class MqttCodec extends ByteToMessageCodec<ControlPacket> {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ControlPacket cp) {
            // core: zero-copy
            ByteBuf buf = cp.toByteBuf();
            // the owner of the buf will transfer to the netty,
            // netty will release the buf after flush it to the wire
            ctx.write(buf, promise);
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
        if (packetLength == INCOMPLETE_PACKET) {// can not decode a packet
            return;
        }
        // core: zero-copy
        ByteBuf incoming = in.readSlice(packetLength);
        ControlPacket cp = ControlPacket.from(incoming);
        if (cp instanceof Publish packet) {
            ReferenceCountUtil.retain(packet.payload());
        }
        out.add(cp);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof DecoderException) {// MqttCodec#decode 中的异常会被包装成 DecodeException
            log.error("malformed packet", cause);
            ctx.channel().close();
        }
        else {
            ctx.fireExceptionCaught(cause);
        }
    }

    public static class V50 extends MqttCodec {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            int packetLength = ControlPacket.tryPickupPacket(in);
            if (packetLength == INCOMPLETE_PACKET) {// can not decode a packet
                return;
            }
            // core: zero-copy
            ByteBuf incoming = in.readSlice(packetLength);
            ControlPacket cp = ControlPacketV50.from(incoming);
            if (cp instanceof Publish packet) {
                ReferenceCountUtil.retain(packet.payload());
            }
            out.add(cp);
        }
    }

    /**
     * <pre>
     *
     * as receiver
     * MqttCodec -> Biz Handler -> this
     *
     * release the Publish.payload that was retain by {@link MqttCodec#decode(ChannelHandlerContext, ByteBuf, List)}
     * </pre>
     */
    public static class Recycler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ControlPacket) {
                if (msg instanceof Publish packet) {
                    // core: zero-copy
                    ReferenceCountUtil.release(packet.payload());
                }
                // no more propagate through the pipeline.
            }
            else {
                ctx.fireChannelRead(msg);
            }
        }
    }
}
