package org.example.mqtt.broker.node;

import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.PingReq;
import org.example.mqtt.model.PingResp;
import org.example.mqtt.session.Session;

/**
 * @author zhanfeng.zhang
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
        if (!(msg instanceof ControlPacket)) {
            log.error("channelRead msg is not ControlPacket, now close the Session and channel");
            closeSession(ctx);
            return;
        }
        ControlPacket cp = (ControlPacket) msg;
        channelRead0(ctx, cp);
        // fireChannelRead if some plugin need use the ControlPacket just before release the ControlPacket
        // io.netty.channel.DefaultChannelPipeline.TailContext#channelRead
        // will release the ByteBuf retained from {@link MqttCodec#decode(ChannelHandlerContext, ByteBuf, List)}
        ctx.fireChannelRead(cp);
    }

    private void closeSession(ChannelHandlerContext ctx) {
        if (session != null) {
            broker.closeSession(session);
        }
        ctx.channel().close();
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
        if (cp instanceof Connect) {
            // A Client can only send the CONNECT Packet once over a Network Connection.
            // The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation
            // and disconnect the Client
            if (session != null) {
                log.error("Client({}) send Connect packet more than once, now close session.", csci());
                closeSession(ctx);
                return;
            }
            Connect connect = (Connect) cp;
            log.debug("Client receive Connect-> cId: {}, packet: {}", connect.clientIdentifier(), connect);
            // The Server MUST respond to the CONNECT Packet
            // with a CONNACK return code 0x01 (unacceptable protocol level) and then
            // disconnect the Client if the Protocol Level is not supported by the Server
            Set<Integer> supportProtocolLevel = broker.supportProtocolLevel();
            if (!supportProtocolLevel.contains(connect.protocolLevel())) {
                log.error("not support protocol level, now send ConnAck and close channel");
                ctx.writeAndFlush(ConnAck.notSupportProtocolLevel());
                closeSession(ctx);
                return;
            }
            // authenticate
            int authenticate = authenticator.authenticate(connect);
            if (authenticate != Authenticator.AUTHENTICATE_SUCCESS) {
                log.error("Connect authenticate failed, now send ConnAck and close channel. {}", authenticate);
                ctx.writeAndFlush(ConnAck.from(authenticate));
                closeSession(ctx);
                return;
            }
            // now accept the 'Connect'
            // keep alive
            if (connect.keepAlive() > 0) {
                addClientKeepAliveHandler(ctx, connect.keepAlive());
            }
            ConnAck connAck = doHandleConnect(connect);
            ctx.writeAndFlush(connAck)
                .addListener(LOG_ON_FAILURE)
                .addListener(FIRE_EXCEPTION_ON_FAILURE)
                .addListener(f -> {
                    this.session.open(ctx.channel(), broker);
                    log.debug("client({}) Connect accepted: {}", connect.clientIdentifier(), connect);
                })
            ;
        }
        else if (cp instanceof PingReq) {
            // no need to pass the packet to the session
            ctx.writeAndFlush(PingResp.from());
        }
        else {
            // let the session handle the packet
            session.onPacket(cp);
        }
    }

    protected ConnAck doHandleConnect(Connect connect) {
        ConnAck connAck = ConnAck.accepted();
        String cId = connect.clientIdentifier();
        ServerSession preSession = broker.session(cId);
        if (preSession != null && preSession.isBound()) {
            log.debug("Client({}) exist bound Session: {}", cId, preSession);
            //  If the ClientId represents a Client already connected to the Server then the Server MUST
            //  disconnect the existing Client
            // preSession.close();
            preSession.channel().close();
            broker.closeSession(preSession);
            // query again
            preSession = broker.session(cId);
            log.debug("Client({}) close exist bound Session: {}", cId, preSession);
        }
        if (connect.cleanSession()) {
            log.debug("Client({}) need a (cleanSession=1) Session, Broker now has Session: {}", cId, preSession);
            if (preSession != null) {
                broker.closeSession(preSession);
                if (log.isDebugEnabled()) {
                    log.debug("Client({}) closed the old Session, Broker now has Session: {}", cId, broker.session(cId));
                }
            }
            this.session = broker.createSession(connect);
            log.debug("Client({}) need a (cleanSession=1) Session, new Session created: {}", cId, this.session);
        }
        else {
            log.debug("Client({}) need a (cleanSession=0) Session, Broker has Session: {}", cId, preSession);
            if (preSession == null) {
                this.session = broker.createSession(connect);
                log.debug("Client({}) need a (cleanSession=0) Session, new Session created", cId);
            }
            else {
                connAck = ConnAck.acceptedWithStoredSession();
                this.session = ((DefaultServerSession) preSession).reInitWith(connect);
                log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", cId, this.session);
            }
        }
        return connAck;
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
            broker.closeSession(session);
        }
        super.channelInactive(ctx);
    }

    private String csci() {
        return Optional.ofNullable(session).map(Session::clientIdentifier).orElse(null);
    }

    protected Broker broker() {
        return broker;
    }

}
