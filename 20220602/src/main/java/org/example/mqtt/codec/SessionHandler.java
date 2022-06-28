package org.example.mqtt.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.Session;
import org.example.mqtt.model.*;

import java.util.Set;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@Slf4j
@RequiredArgsConstructor
public class SessionHandler extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "sessionHandler";
    public static final String ACTIVE_IDLE_TIMEOUT_HANDLER = "activeIdleTimeoutHandler";

    private Session session;
    private final Broker broker;
    private final Authenticator authenticator;
    private final int activeIdleTimeoutSecond;

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
            if (session != null) {
                session.close();
            }
            ctx.close();
            return;
        }
        ControlPacket cp = (ControlPacket) msg;
        // After a Network Connection is established by a Client to a Server,
        // the first Packet sent from the Client to the Server MUST be a CONNECT Packet
        if (session == null && !(cp instanceof Connect)) {
            log.error("channelRead the first Packet is not Connect, now close channel");
            ctx.close();
        }
        // the first Connect Packet
        if (cp instanceof Connect) {
            Connect connect = (Connect) cp;
            // A Client can only send the CONNECT Packet once over a Network Connection.
            // The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation
            // and disconnect the Client
            if (session != null) {
                log.error("channelRead send Connect packet more than once, now close channel");
                ctx.close();
            }
            // The Server MUST respond to the CONNECT Packet with a CONNACK
            // return code 0x01 (unacceptable protocol level) and then
            // disconnect the Client if the Protocol Level is not supported by the Server
            Set<Integer> supportProtocolLevel = broker.supportProtocolLevel();
            if (!supportProtocolLevel.contains(connect.protocolLevel())) {
                log.error("not support protocol level, now send ConnAck and close channel");
                ctx.writeAndFlush(ConnAck.notSupportProtocolLevel());
                ctx.close();
            }
            // authenticate
            int authenticate = authenticator.authenticate(connect);
            if (authenticate != Authenticator.AUTHENTICATE_SUCCESS) {
                log.error("Connect authenticate failed, now send ConnAck and close channel. {}", authenticate);
                ctx.writeAndFlush(new ConnAck(authenticate));
                ctx.close();
            }
            // keep alive
            if (connect.keepAlive() > 0) {
                addClientKeepAliveHandler(ctx, connect.keepAlive());
            }
            // broker try accept the Connect packet
            Session accepted = broker.accepted(connect, ctx.channel());
            if (session != null) {
                this.session = accepted;
            } else {
                // just close the Channel
                log.error("Broker does not accept the Connect, now send an ConnAck and close channel");
                ctx.writeAndFlush(ConnAck.serverUnavailable());
                ctx.close();
            }
        } else if (cp instanceof PingReq) {
            // no need to pass the packet to the session
            ctx.writeAndFlush(new PingResp());
        } else {
            // let the session handle the packet
            session.messageReceived(cp);
        }
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
        log.info("addActiveIdleTimeoutHandler done");
    }

    private void removeActiveIdleTimeoutHandler(ChannelHandlerContext ctx) {
        ctx.pipeline().remove(ACTIVE_IDLE_TIMEOUT_HANDLER);
        log.info("removeActiveIdleTimeoutHandler done");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ReadTimeoutException) {
            log.error("channel read timeout, now close the channel");
        }
        super.exceptionCaught(ctx, cause);
    }

}
