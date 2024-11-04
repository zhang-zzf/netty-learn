package org.example.mqtt.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.PingReq;
import org.example.mqtt.model.PingResp;

@Slf4j
@RequiredArgsConstructor
public class ClientSessionHandler extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = ClientSessionHandler.class.getSimpleName();

    public final ClientSession session;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ControlPacket) {
            ControlPacket cp = (ControlPacket) msg;
            if (cp instanceof PingResp) {
                log.debug("Client({}) receive PingResp", session.clientIdentifier());
                return;
            }
            if (cp instanceof ConnAck && ((ConnAck) cp).connectionAccepted()) {
                addKeepAliveIdleStateHandler(ctx);
            }
            session.onPacket(cp);
            // io.netty.channel.DefaultChannelPipeline.TailContext#channelRead
            // will release the ByteBuf retained from {@link MqttCodec#decode(ChannelHandlerContext, ByteBuf, List)}
            ctx.fireChannelRead(cp);
        }
        else {
            super.channelRead(ctx, msg);
        }
    }

    private void addKeepAliveIdleStateHandler(ChannelHandlerContext ctx) {
        int keepAlive = session.keepAlive();
        IdleStateHandler idle = new IdleStateHandler(0, keepAlive, 0);
        ctx.pipeline().addBefore(HANDLER_NAME, "keepAliveIdleStateHandler", idle);
        log.debug("keepAliveIdleStateHandler added: {}", keepAlive);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("Client({}) channelInactive, now cleanup the Session", session.clientIdentifier());
        session.channelClosed();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Client({}) exceptionCaught, now close the Session", session.clientIdentifier(), cause);
        session.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                ctx.close();
            }
            else if (e.state() == IdleState.WRITER_IDLE) {
                // send ping
                log.debug("Client({}) send PingReq", session.clientIdentifier());
                ctx.writeAndFlush(PingReq.from());
            }
        }
        super.userEventTriggered(ctx, evt);
    }

}
