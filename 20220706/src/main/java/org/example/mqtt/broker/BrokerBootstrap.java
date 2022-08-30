package org.example.mqtt.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.jvm.DefaultBroker;
import org.example.mqtt.codec.Codec;

import javax.net.ssl.SSLException;
import java.io.InputStream;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrap {

    public static void main(String[] args) throws SSLException {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, new DefaultThreadFactory("netty-worker"));
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("netty-boss"));
        final Broker broker = new DefaultBroker();
        // 配置 bootstrap
        final int port = 1883;
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ServerSessionHandler sessionHandler = new ServerSessionHandler(broker, packet -> 0x00, 3);
                        ch.pipeline().addLast(new Codec()).addLast(ServerSessionHandler.HANDLER_NAME, sessionHandler);
                    }
                })
                .bind(port)
                .addListener((ChannelFutureListener) future -> {
                    log.info("MQTT server listened at {}", port);
                })
                .channel().closeFuture().addListener((ChannelFutureListener) future -> {
                    log.info("server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
        // 配置 bootstrap
        final int sslPort = 8883;
        InputStream key = ClassLoader.getSystemResourceAsStream("cert/netty.zhanfengzhang.top.pkcs8.key");
        InputStream cert = ClassLoader.getSystemResourceAsStream("cert/netty.zhanfengzhang.top.pem");
        SslContext sslCtx = SslContextBuilder.forServer(cert, key).build();
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new SecureMqttServerInitializer(broker, packet -> 0x00, sslCtx, 3))
                .bind(sslPort).addListener(f -> log.info("MQTT TLS server listened at {}", sslPort))
                .channel().closeFuture().addListener(f -> {
                    log.info("MQTT TLS server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
    }

}
