package org.example.mqtt.broker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.codec.Codec;
import org.example.mqtt.model.*;
import org.example.mqtt.session.Session;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@Slf4j
@RequiredArgsConstructor
public class ServerSessionHandler extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "sessionHandler";
    public static final String ACTIVE_IDLE_TIMEOUT_HANDLER = "activeIdleTimeoutHandler";

    private ServerSession session;
    private final Broker broker;
    private final Authenticator authenticator;
    private final int activeIdleTimeoutSecond;
    private ReadTimeoutHandler activeIdleTimeoutHandler;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // If the Server does not receive a CONNECT Packet
        // within a reasonable amount of time after the Network Connection is established,
        // the Server SHOULD close the connection
        addActiveIdleTimeoutHandler(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // receive a packet, remove the activeIdleTimeoutHandler
        removeActiveIdleTimeoutHandler(ctx);
        if (!(msg instanceof ControlPacket)) {
            log.error("channelRead msg is not ControlPacket, now close the Session and channel");
            closeSession(ctx);
            return;
        }
        ControlPacket cp = (ControlPacket) msg;
        try {
            channelRead0(ctx, cp);
            // fireChannelRead if need just before release the ControlPacket
            ctx.fireChannelRead(cp);
        } finally {
            /**
             * release the ByteBuf retained from {@link Codec#decode(ChannelHandlerContext, ByteBuf, List)}
             */
            cp._buf().release();
        }
    }

    private boolean closeSession(ChannelHandlerContext ctx) throws Exception {
        if (existSession()) {
            session.close();
        } else {
            ctx.close();
        }
        return true;
    }

    /**
     * @return true if channelRead0 close the session otherwise false;
     */
    private boolean channelRead0(ChannelHandlerContext ctx, ControlPacket cp) throws Exception {
        // After a Network Connection is established by a Client to a Server,
        // the first Packet sent from the Client to the Server MUST be a CONNECT Packet
        if (session == null && !(cp instanceof Connect)) {
            log.error("channelRead the first Packet is not Connect, now close channel");
            return closeSession(ctx);
        }
        // the first Connect Packet
        if (cp instanceof Connect) {
            // A Client can only send the CONNECT Packet once over a Network Connection.
            // The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation
            // and disconnect the Client
            if (existSession()) {
                log.error("channelRead send Connect packet more than once, now close session");
                return closeSession(ctx);
            }
            Connect connect = (Connect) cp;
            // The Server MUST respond to the CONNECT Packet
            // with a CONNACK return code 0x01 (unacceptable protocol level) and then
            // disconnect the Client if the Protocol Level is not supported by the Server
            Set<Integer> supportProtocolLevel = broker.supportProtocolLevel();
            if (!supportProtocolLevel.contains(connect.protocolLevel())) {
                log.error("not support protocol level, now send ConnAck and close channel");
                ctx.writeAndFlush(ConnAck.notSupportProtocolLevel());
                return closeSession(ctx);
            }
            // authenticate
            int authenticate = authenticator.authenticate(connect);
            if (authenticate != Authenticator.AUTHENTICATE_SUCCESS) {
                log.error("Connect authenticate failed, now send ConnAck and close channel. {}", authenticate);
                ctx.writeAndFlush(ConnAck.from(authenticate));
                return closeSession(ctx);
            }
            // keep alive
            if (connect.keepAlive() > 0) {
                addClientKeepAliveHandler(ctx, connect.keepAlive());
            }
            // now accept the Connect
            ConnAck connAck = ConnAck.accepted();
            ServerSession preSession = broker.session(connect.clientIdentifier());
            if (connect.cleanSession() && preSession != null) {
                preSession.close();
            }
            if (!connect.cleanSession() && preSession != null) {
                this.session = preSession;
                connAck = ConnAck.acceptedWithStoredSession();
            }
            if (this.session == null) {
                DefaultServerSession newSession = newServerSession(connect);
                newSession.cleanSession(connect.cleanSession());
                this.session = newSession;
            }
            ctx.writeAndFlush(connAck).addListener((ChannelFutureListener) future -> {
                // bind channel
                try {
                    this.session.bind(ctx.channel());
                    this.session.register(broker);
                } catch (Exception e) {
                    log.error("Session({}) unexpected exception when bind", session.clientIdentifier(), e);
                    closeSession(ctx);
                }
            });
            log.info("client({}) Connect accepted: {}", connect.clientIdentifier(), connect);
        } else if (cp instanceof PingReq) {
            // no need to pass the packet to the session
            ctx.writeAndFlush(PingResp.from());
        } else {
            // let the session handle the packet
            session.messageReceived(cp);
        }
        return false;
    }

    protected DefaultServerSession newServerSession(Connect connect) {
        return new DefaultServerSession(connect.clientIdentifier());
    }

    private void addClientKeepAliveHandler(ChannelHandlerContext ctx, int keepAlive) {
        // If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
        // within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the
        // Client as if the network had failed
        ReadTimeoutHandler handler = new ReadTimeoutHandler(keepAlive * 3 / 2);
        ctx.pipeline().addBefore(HANDLER_NAME, "clientKeepAliveHandler", handler);
        log.info("addClientKeepAliveHandler done");
    }

    private void addActiveIdleTimeoutHandler(ChannelHandlerContext ctx) {
        ReadTimeoutHandler handler = new ReadTimeoutHandler(activeIdleTimeoutSecond);
        ctx.pipeline().addBefore(HANDLER_NAME, ACTIVE_IDLE_TIMEOUT_HANDLER, handler);
        this.activeIdleTimeoutHandler = handler;
        log.info("addActiveIdleTimeoutHandler done");
    }

    private void removeActiveIdleTimeoutHandler(ChannelHandlerContext ctx) {
        if (this.activeIdleTimeoutHandler == null) {
            return;
        }
        ctx.pipeline().remove(ACTIVE_IDLE_TIMEOUT_HANDLER);
        this.activeIdleTimeoutHandler = null;
        log.info("removeActiveIdleTimeoutHandler done");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("client({}) exceptionCaught. now close the session", curSessionClientIdentifier(), cause);
        closeSession(ctx);
    }

    private boolean existSession() {
        return session != null;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Session({}) channelInactive", curSessionClientIdentifier());
        closeSession(ctx);
        super.channelInactive(ctx);
    }

    private String curSessionClientIdentifier() {
        return Optional.ofNullable(session).map(Session::clientIdentifier).orElse(null);
    }

}
