package org.github.zzf.mqtt.protocol.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.util.List;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.Publish;

/**
 * <pre>
 *
 * as receiver
 * MqttCodec -> Biz Handler -> this
 *
 * release the Publish.payload that was retain by {@link MqttCodec#decode(ChannelHandlerContext, ByteBuf, List)}
 * </pre>
 */
public class ControlPacketRecycler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
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
