package org.example.mqtt.broker;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import lombok.RequiredArgsConstructor;
import org.example.mqtt.codec.Codec;

@RequiredArgsConstructor
public class SecureMqttServerInitializer extends ChannelInitializer<SocketChannel> {

    private final Broker broker;
    private final Authenticator authenticator;
    private final SslContext sslCtx;
    private final int activeIdleTimeoutSecond;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast(sslCtx.newHandler(ch.alloc()))
                .addLast(new Codec())
                .addLast(ServerSessionHandler.HANDLER_NAME,
                        new ServerSessionHandler(broker, authenticator, activeIdleTimeoutSecond))
        ;

    }

}
