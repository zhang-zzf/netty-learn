package org.example.mqtt.broker.codec;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import lombok.RequiredArgsConstructor;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class MqttServerInitializer extends ChannelInitializer<SocketChannel> {

    final Supplier<DefaultServerSessionHandler> handlerSupplier;

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast(new MqttCodec())
                .addLast(DefaultServerSessionHandler.HANDLER_NAME, handlerSupplier.get())
        ;

    }

}
