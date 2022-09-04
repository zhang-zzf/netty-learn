package org.example.mqtt.bootstrap;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import lombok.RequiredArgsConstructor;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSessionHandler;

@RequiredArgsConstructor
public class MqttServerInitializer extends ChannelInitializer<SocketChannel> {

    private final Broker broker;
    private final Authenticator authenticator;
    private final int activeIdleTimeoutSecond;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast(new MqttCodec())
                .addLast(ServerSessionHandler.HANDLER_NAME,
                        new ServerSessionHandler(broker, authenticator, activeIdleTimeoutSecond))
        ;

    }

}
