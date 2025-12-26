package org.github.zzf.mqtt.server.codec.websocket;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.ssl.SslContext;
import lombok.RequiredArgsConstructor;
import org.github.zzf.mqtt.protocol.codec.ControlPacketRecycler;
import org.github.zzf.mqtt.protocol.codec.MqttCodec;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.server.DefaultServerSessionHandler;

@RequiredArgsConstructor
public class MqttOverSecureWebsocketServerInitializer extends ChannelInitializer<SocketChannel> {

    final String subProtocols = "mqtt";
    private final String websocketPath;
    private final SslContext sslCtx;
    private final Broker broker;
    private final int activeIdleTimeoutSecond;

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
                // SSL
                .addFirst(sslCtx.newHandler(ch.alloc()))
                // http handler
                .addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(65536))
                // websocket handler
                .addLast(new WebSocketServerCompressionHandler())
                .addLast(new WebSocketServerProtocolHandler(websocketPath, subProtocols, true))
                // WebSocketFrameCodec
                // inbound:     BinaryWebSocketFrame -> ByteBuf
                // outbound:    ByteBuf -> BinaryWebSocketFrame
                .addLast(new WebSocketFrameCodec())
                // mqtt codec
                .addLast(new MqttCodec())
                // mqtt SessionHandler
                .addLast(DefaultServerSessionHandler.HANDLER_NAME, new DefaultServerSessionHandler(broker, activeIdleTimeoutSecond))
                .addLast(new ControlPacketRecycler())
        ;

    }

}
