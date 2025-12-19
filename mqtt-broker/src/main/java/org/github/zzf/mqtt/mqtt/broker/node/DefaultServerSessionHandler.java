package org.github.zzf.mqtt.mqtt.broker.node;

import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.mqtt.broker.Authenticator;
import org.github.zzf.mqtt.mqtt.broker.Broker;
import org.github.zzf.mqtt.mqtt.broker.ServerSession;
import org.github.zzf.mqtt.protocol.codec.MqttCodec;
import org.github.zzf.mqtt.protocol.model.ConnAck;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.PingReq;
import org.github.zzf.mqtt.protocol.model.PingResp;
import org.github.zzf.mqtt.protocol.session.AbstractSession;
import org.github.zzf.mqtt.protocol.session.Session;

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
    private final Authenticator authenticator;
    private final int activeIdleTimeoutSecond;
    private ReadTimeoutHandler activeIdleTimeoutHandler;

    private final ChannelFutureListener SEND_PACKET_AFTER_CONNACK = future -> {
        if (future.isSuccess() && session instanceof AbstractSession as) {
            as.connected();
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
            closeSession(ctx);
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
            closeSession(ctx);
            return;
        }
        // the first Connect Packet
        if (cp instanceof Connect connect) {
            // A Client can only send the CONNECT Packet once over a Network Connection.
            // The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation
            // and disconnect the Client
            if (session != null) {
                log.error("Client({}) send Connect packet more than once, now close session.", csci());
                closeSession(ctx);
                return;
            }
            log.debug("Server receive Connect from client({}) -> {}", connect.clientIdentifier(), connect);
            // The Server MUST respond to the CONNECT Packet
            // with a CONNACK return code 0x01 (unacceptable protocol level) and then
            // disconnect the Client if the Protocol Level is not supported by the Server
            Set<Integer> supportProtocolLevel = broker.supportProtocolLevel();
            if (!supportProtocolLevel.contains(connect.protocolLevel())) {
                log.error("Server not support protocol level, now send ConnAck and close channel to client({})", connect.clientIdentifier());
                ctx.writeAndFlush(ConnAck.notSupportProtocolLevel());
                closeSession(ctx);
                return;
            }
            // authenticate
            int authenticate = authenticator.authenticate(connect);
            if (authenticate != Authenticator.AUTHENTICATE_SUCCESS) {
                log.error("Server authenticate Connect from client({}) failed, now send ConnAck and close channel -> {}", connect.clientIdentifier(), authenticate);
                ctx.writeAndFlush(new ConnAck(authenticate));
                closeSession(ctx);
                return;
            }
            // now accept the 'Connect'
            // keep alive
            if (connect.keepAlive() > 0) {
                addClientKeepAliveHandler(ctx, connect.keepAlive());
            }
            ConnAck connAck = doHandleConnect(connect, ctx);
            ctx.writeAndFlush(connAck)
                .addListener(LOG_ON_FAILURE)
                .addListener(FIRE_EXCEPTION_ON_FAILURE)
                .addListener(f -> log.debug("Client({}) Connect accepted: {}", connect.clientIdentifier(), connect))
                .addListener(SEND_PACKET_AFTER_CONNACK)
            ;
        }
        else if (cp instanceof PingReq) {
            // no need to pass the packet to the session
            ctx.writeAndFlush(new PingResp());
        }
        else {
            // let the session handle the packet
            session.onPacket(cp);
        }
    }

    protected ConnAck doHandleConnect(Connect connect, ChannelHandlerContext ctx) {
        this.session = new DefaultServerSession(connect, ctx.channel(), broker);
        if (broker.attachSession(this.session)) {
            return ConnAck.acceptedWithStoredSession();
        }
        return ConnAck.accepted();
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
        closeSession(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("Client({}) channelInactive", csci());
        if (session != null) {
            session.close();
        }
        super.channelInactive(ctx);
    }

    private String csci() {
        return Optional.ofNullable(session).map(Session::clientIdentifier).orElse(null);
    }

    protected Broker broker() {
        return broker;
    }

    private void closeSession(ChannelHandlerContext ctx) {
        if (session != null) {
            session.close();
        }
        else {
            ctx.channel().close();
        }
    }

}
