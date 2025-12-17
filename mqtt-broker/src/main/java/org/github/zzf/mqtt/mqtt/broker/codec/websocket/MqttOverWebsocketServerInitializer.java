package org.github.zzf.mqtt.mqtt.broker.codec.websocket;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import lombok.RequiredArgsConstructor;
import org.github.zzf.mqtt.mqtt.broker.node.DefaultServerSessionHandler;
import org.github.zzf.mqtt.protocol.codec.MqttCodec;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class MqttOverWebsocketServerInitializer extends ChannelInitializer<SocketChannel> {

    final String subProtocols = "mqtt";
    private final String websocketPath;
    private final Supplier<DefaultServerSessionHandler> handlerSupplier;

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
                .addLast(DefaultServerSessionHandler.HANDLER_NAME, handlerSupplier.get())
        ;

    }

}
