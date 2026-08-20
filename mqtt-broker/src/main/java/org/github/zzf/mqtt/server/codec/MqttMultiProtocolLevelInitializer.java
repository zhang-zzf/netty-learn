package org.github.zzf.mqtt.server.codec;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.INCOMPLETE_PACKET;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.readByte;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.readUTF8String;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.readVariableByteInteger;
import static org.github.zzf.mqtt.server.DefaultServerSessionHandler.HANDLER_NAME;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.codec.MqttCodec;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Connect.V50;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.server.DefaultServerSessionHandler;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2026-01-31
 */
@Slf4j
@RequiredArgsConstructor
public class MqttMultiProtocolLevelInitializer extends ChannelInboundHandlerAdapter {

    public static final String MQTT_MULTI_PROTOCOL_LEVEL_INITIALIZER = "MqttMultiProtocolLevelInitializer";

    public static final String ACTIVE_IDLE_TIMEOUT_HANDLER = "activeIdleTimeoutHandler";

    final int activeIdleTimeoutSecond;
    final Broker broker;
    ReadTimeoutHandler activeIdleTimeoutHandler;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // If the Server does not receive a CONNECT Packet
        // within a reasonable amount of time after the Network Connection is established,
        // the Server SHOULD close the connection
        addActiveIdleTimeoutHandler(ctx);
        super.channelActive(ctx);
    }

    private void addActiveIdleTimeoutHandler(ChannelHandlerContext ctx) {
        ReadTimeoutHandler handler = new ReadTimeoutHandler(activeIdleTimeoutSecond);
        ctx.pipeline().addFirst(ACTIVE_IDLE_TIMEOUT_HANDLER, handler);
        this.activeIdleTimeoutHandler = handler;
        log.debug("addActiveIdleTimeoutHandler done");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf in)) {
            ctx.channel().close();
            return;
        }
        // the first packet must be Connect
        int packetLength = ControlPacket.tryPickupPacket(in);
        if (packetLength == INCOMPLETE_PACKET) {// can not decode a packet
            return;
        }
        /* After a Network Connection is established by a Client to a Server,
        the first Packet sent from the Client to the Server MUST be a CONNECT Packet
        */
        byte byte0 = in.getByte(in.readerIndex());
        if (byte0 != Connect.CONNECT) {
            log.error("Server -> Client() >> CloseSession: first Packet is not Connect");
            ctx.channel().close();
            return;
        }
        // receive Connect packet, remove the activeIdleTimeoutHandler
        removeActiveIdleTimeoutHandler(ctx);
        //
        // core: zero-copy
        doSwitchMqttProtocol(ctx, in.slice());
        ctx.fireChannelRead(in);
        ctx.pipeline().remove(this);
    }

    private void doSwitchMqttProtocol(ChannelHandlerContext ctx, ByteBuf incoming) {
        readByte(incoming);// byte1
        readVariableByteInteger(incoming);// Remaining Length
        readUTF8String(incoming);// Protocol Name
        byte protocolLevel = readByte(incoming);// Protocol Level
        ChannelPipeline pipeline = ctx.pipeline();
        if (protocolLevel == Connect.PROTOCOL_LEVEL_3_1_1) {
            pipeline.addAfter(MQTT_MULTI_PROTOCOL_LEVEL_INITIALIZER, "MqttCodec", new MqttCodec());
            pipeline.addAfter("MqttCodec", HANDLER_NAME, new DefaultServerSessionHandler(broker, ctx.channel()));
            pipeline.addAfter(HANDLER_NAME, "MqttCode.Recycler", new MqttCodec.Recycler());
        }
        else if (protocolLevel == V50.PROTOCOL_LEVEL_5_0) {
            pipeline.addAfter(MQTT_MULTI_PROTOCOL_LEVEL_INITIALIZER, "MqttCodec.V50", new MqttCodec.V50());
            pipeline.addAfter("MqttCodec.V50", HANDLER_NAME, new DefaultServerSessionHandler.V50(broker, ctx.channel()));
            pipeline.addAfter(HANDLER_NAME, "MqttCode.Recycler", new MqttCodec.Recycler());
        }
        else {
            log.error("Server -> Client() >> CloseSession : the protocolLevel is not support");
            ctx.channel().close();
        }
    }

    private void removeActiveIdleTimeoutHandler(ChannelHandlerContext ctx) {
        if (this.activeIdleTimeoutHandler == null) {
            return;
        }
        ctx.pipeline().remove(ACTIVE_IDLE_TIMEOUT_HANDLER);
        this.activeIdleTimeoutHandler = null;
        log.debug("removeActiveIdleTimeoutHandler done");
    }

}
