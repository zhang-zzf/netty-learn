package org.github.zzf.mqtt.server;

import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.ConnAck;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Connect.AuthenticationException;
import org.github.zzf.mqtt.protocol.model.Connect.UnSupportProtocolLevelException;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.session.AbstractSession;
import org.github.zzf.mqtt.protocol.session.Session;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.ServerSession;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/06/28
 */
@Slf4j
@RequiredArgsConstructor
public class DefaultServerSessionHandler extends ChannelInboundHandlerAdapter {

    public static final ChannelFutureListener LOG_ON_FAILURE = future -> {
        if (!future.isSuccess()) {
            log.error("Channel(" + future.channel() + ").writeAndFlush failed.", future.cause());
        }
    };

    public static final String HANDLER_NAME = DefaultServerSessionHandler.class.getSimpleName();
    public static final String ACTIVE_IDLE_TIMEOUT_HANDLER = "activeIdleTimeoutHandler";

    protected ServerSession session;
    private final Broker broker;
    private final int activeIdleTimeoutSecond;
    private ReadTimeoutHandler activeIdleTimeoutHandler;

    private final ChannelFutureListener SESSION_ESTABLISHED_CALLBACK = future -> {
        if (future.isSuccess() && session instanceof AbstractSession as) {
            as.established();
        }
    };

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // If the Server does not receive a CONNECT Packet
        // within a reasonable amount of time after the Network Connection is established,
        // the Server SHOULD close the connection
        addActiveIdleTimeoutHandler(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // receive a packet, remove the activeIdleTimeoutHandler
        removeActiveIdleTimeoutHandler(ctx);
        if (!(msg instanceof ControlPacket cp)) {
            log.error("channelRead msg is not ControlPacket, now close the Session and channel");
            ctx.channel().close();
            return;
        }
        channelRead0(ctx, cp);
        /**
         fireChannelRead if some plugin need use the ControlPacket just before release the ControlPacket
         */
        ctx.fireChannelRead(cp);
    }

    private void channelRead0(ChannelHandlerContext ctx, ControlPacket cp) {
        // After a Network Connection is established by a Client to a Server,
        // the first Packet sent from the Client to the Server MUST be a CONNECT Packet
        if (session == null && !(cp instanceof Connect)) {
            log.error("channelRead the first Packet is not Connect, now close channel");
            ctx.channel().close();
            return;
        }
        // the first Connect Packet
        if (cp instanceof Connect connect) {
            // A Client can only send the CONNECT Packet once over a Network Connection.
            // The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation
            // and disconnect the Client
            if (session != null) {
                log.error("Client({}) send Connect packet more than once, now close session.", csci());
                ctx.channel().close();
                return;
            }
            // now accept the 'Connect'
            try {
                session = broker.onConnect(connect, ctx.channel());
            } catch (UnSupportProtocolLevelException e) {
                log.info("Server not support protocol level, now send ConnAck and close channel to client({})", connect.clientIdentifier());
                ctx.channel().writeAndFlush(ConnAck.notSupportProtocolLevel()).channel().close();
            } catch (AuthenticationException e) {
                int authenticate = e.getAuthenticate();
                log.info("Server authenticate Connect from client({}) failed, now send ConnAck and close channel -> {}", connect.clientIdentifier(), authenticate);
                ctx.channel().writeAndFlush(new ConnAck(authenticate)).channel().close();
            } catch (IllegalArgumentException e) {
                log.error("Client({}) Connect failed: {}", connect.clientIdentifier(), e.getMessage());
                ctx.channel().close();
            }
            // send ConnAck
            ConnAck connAck = session.isResumed() ? ConnAck.acceptedWithStoredSession() : ConnAck.accepted();
            ctx.writeAndFlush(connAck)
                .addListener(LOG_ON_FAILURE)
                .addListener(FIRE_EXCEPTION_ON_FAILURE)
                .addListener(f -> log.debug("Client({}) Connect accepted: {}", connect.clientIdentifier(), connect))
                .addListener(SESSION_ESTABLISHED_CALLBACK)
            ;
            // keep alive
            if (connect.keepAlive() > 0) {
                addClientKeepAliveHandler(ctx, connect.keepAlive());
            }
        }
        else {
            // let the session handle the packet
            session.onPacket(cp);
        }
    }

    private void addClientKeepAliveHandler(ChannelHandlerContext ctx, int keepAlive) {
        // If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
        // within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the
        // Client as if the network had failed
        ReadTimeoutHandler handler = new ReadTimeoutHandler(keepAlive * 3 / 2);
        ctx.pipeline().addBefore(HANDLER_NAME, "clientKeepAliveHandler", handler);
        log.debug("addClientKeepAliveHandler done");
    }

    private void addActiveIdleTimeoutHandler(ChannelHandlerContext ctx) {
        ReadTimeoutHandler handler = new ReadTimeoutHandler(activeIdleTimeoutSecond);
        ctx.pipeline().addBefore(HANDLER_NAME, ACTIVE_IDLE_TIMEOUT_HANDLER, handler);
        this.activeIdleTimeoutHandler = handler;
        log.debug("addActiveIdleTimeoutHandler done");
    }

    private void removeActiveIdleTimeoutHandler(ChannelHandlerContext ctx) {
        if (this.activeIdleTimeoutHandler == null) {
            return;
        }
        ctx.pipeline().remove(ACTIVE_IDLE_TIMEOUT_HANDLER);
        this.activeIdleTimeoutHandler = null;
        log.debug("removeActiveIdleTimeoutHandler done");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Client({" + csci() + "}) exceptionCaught. now close the Channel -> channel: {}", ctx.channel());
        log.error("Client({" + csci() + "}) exceptionCaught. now close the session", cause);
        ctx.channel().close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("Client({}) channelInactive", csci());
        super.channelInactive(ctx);
    }

    private String csci() {
        return Optional.ofNullable(session).map(Session::clientIdentifier).orElse(null);
    }

    protected Broker broker() {
        return broker;
    }

}
