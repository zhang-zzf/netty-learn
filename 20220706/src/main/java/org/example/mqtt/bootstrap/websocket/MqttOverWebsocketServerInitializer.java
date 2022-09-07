package org.example.mqtt.bootstrap.websocket;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import lombok.RequiredArgsConstructor;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSessionHandler;
import org.example.mqtt.bootstrap.MqttCodec;

@RequiredArgsConstructor
public class MqttOverWebsocketServerInitializer extends ChannelInitializer<SocketChannel> {

    final String subProtocols = "mqtt";
    private final String websocketPath;
    private final Broker broker;
    private final Authenticator authenticator;
    private final int activeIdleTimeoutSecond;

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
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
                .addLast(ServerSessionHandler.HANDLER_NAME,
                        new ServerSessionHandler(broker, authenticator, activeIdleTimeoutSecond))
        ;

    }

}