package org.example.mqtt.bootstrap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.jvm.DefaultBroker;
import org.example.mqtt.bootstrap.websocket.MqttOverSecureWebsocketServerInitializer;
import org.example.mqtt.bootstrap.websocket.MqttOverWebsocketServerInitializer;

import javax.net.ssl.SSLException;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrap {

    public static void main(String[] args) throws SSLException {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, new DefaultThreadFactory("netty-worker"));
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(4, new DefaultThreadFactory("netty-boss"));
        final Broker broker = new DefaultBroker();
        // 配置 bootstrap
        final int port = 1883;
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new MqttServerInitializer(broker, packet -> 0x00, 3))
                .bind(port).addListener((ChannelFutureListener) future -> {
                    log.info("MQTT server listened at {}", port);
                })
                .channel().closeFuture().addListener((ChannelFutureListener) future -> {
                    log.info("MQTT server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
        // 配置 tls bootstrap
        final int sslPort = 8883;
        SslContext sslCtx = SslContextBuilder.forServer(
                        ClassLoader.getSystemResourceAsStream("cert/netty.zhanfengzhang.top.pem"),
                        ClassLoader.getSystemResourceAsStream("cert/netty.zhanfengzhang.top.pkcs8.key"))
                .build();
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
        // 配置 websocket bootstrap
        final int websocketPort = 8080;
        MqttOverWebsocketServerInitializer mqttOverWebsocket =
                new MqttOverWebsocketServerInitializer("/mqtt", broker, packet -> 0x00, 3);
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(mqttOverWebsocket)
                .bind(websocketPort).addListener(f -> log.info("MQTT over Websocket server listened at {}", websocketPort))
                .channel().closeFuture().addListener(f -> {
                    log.info("MQTT over Websocket server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
        // 配置 websocket tls bootstrap
        final int websocketSslPort = 8443;
        final SslContext websocketSslCtx = SslContextBuilder.forServer(
                        ClassLoader.getSystemResourceAsStream("cert/netty.zhanfengzhang.top.pem"),
                        ClassLoader.getSystemResourceAsStream("cert/netty.zhanfengzhang.top.pkcs8.key"))
                .build();
        MqttOverSecureWebsocketServerInitializer mqttOverSecureWebsocket =
                new MqttOverSecureWebsocketServerInitializer("/mqtt", broker, packet -> 0x00, websocketSslCtx, 3);
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(mqttOverSecureWebsocket)
                .bind(websocketSslPort)
                .addListener(f -> log.info("MQTT over Websocket(TLS) server listened at {}", websocketSslPort))
                .channel().closeFuture().addListener(f -> {
                    log.info("MQTT over Websocket(TLS) server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
    }

}
