package org.github.zzf.mqtt.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.ServerSession;
import org.github.zzf.mqtt.protocol.session.Session;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/06/28
 */
@Slf4j
@RequiredArgsConstructor
public class DefaultServerSessionHandler extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = DefaultServerSessionHandler.class.getSimpleName();

    final ServerSession session;

    public DefaultServerSessionHandler(Broker broker, Channel channel) {
        this(new DefaultServerSession(broker, channel));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ControlPacket cp)) {
            log.error("channelRead msg is not ControlPacket, now close the Session and channel");
            ctx.channel().close();
            return;
        }
        if (cp instanceof Connect connect && connect.keepAlive() > 0) {
            addClientKeepAliveHandler(ctx, connect.keepAlive());
        }
        // let the session handle the packet
        session.sessionRead(cp);
        /* fireChannelRead if some plugin need use the ControlPacket just before release the ControlPacket */
        ctx.fireChannelRead(cp);
    }

    void addClientKeepAliveHandler(ChannelHandlerContext ctx, int keepAlive) {
        // If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
        // within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the
        // Client as if the network had failed
        ReadTimeoutHandler handler = new ReadTimeoutHandler(keepAlive * 3 / 2);
        ctx.pipeline().addBefore(HANDLER_NAME, "clientKeepAliveHandler", handler);
        log.debug("addClientKeepAliveHandler done");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Session({}) exceptionCaught. now close the Channel -> channel: {}", csci(), ctx.channel(), cause);
        ctx.channel().close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("Session({}) channelInactive", csci());
        session.sessionInactive();
        super.channelInactive(ctx);
    }

    private String csci() {
        return Optional.ofNullable(session).map(Session::clientIdentifier).orElse(null);
    }

    public static class V50 extends DefaultServerSessionHandler {

        public V50(Broker broker, Channel channel) {
            super(new DefaultServerSession.V50(broker, channel));
        }

    }

}
