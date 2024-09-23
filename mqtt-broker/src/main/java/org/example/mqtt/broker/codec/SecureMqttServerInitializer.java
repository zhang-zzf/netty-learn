package org.example.mqtt.broker.codec;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import lombok.RequiredArgsConstructor;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class SecureMqttServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private final Supplier<DefaultServerSessionHandler> handlerSupplier;

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast(sslCtx.newHandler(ch.alloc()))
                .addLast(new MqttCodec())
                .addLast(DefaultServerSessionHandler.HANDLER_NAME, handlerSupplier.get())
        ;
    }

}
