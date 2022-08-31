package org.example.mqtt.broker.websocket;

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
import org.example.mqtt.codec.Codec;

@RequiredArgsConstructor
public class MqttOverWebsocketServerInitializer extends ChannelInitializer<SocketChannel> {

    final String subProtocols = "mqtt";
    final String websocketPath = "/";
    private final Broker broker;
    private final Authenticator authenticator;
    private final int activeIdleTimeoutSecond;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                // http handler
                .addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(65536))
                // websocket handler
                .addLast(new WebSocketServerCompressionHandler())
                .addLast(new WebSocketServerProtocolHandler(websocketPath, subProtocols, true))
                .addLast(new WebSocketFrameHandler())
                // mqtt codec
                .addLast(new Codec())
                // mqtt SessionHandler
                .addLast(ServerSessionHandler.HANDLER_NAME,
                        new ServerSessionHandler(broker, authenticator, activeIdleTimeoutSecond))
        ;

    }

}
