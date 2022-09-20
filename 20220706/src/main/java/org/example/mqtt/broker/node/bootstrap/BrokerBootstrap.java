package org.example.mqtt.broker.node.bootstrap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.codec.MqttServerInitializer;
import org.example.mqtt.broker.codec.SecureMqttServerInitializer;
import org.example.mqtt.broker.codec.websocket.MqttOverSecureWebsocketServerInitializer;
import org.example.mqtt.broker.codec.websocket.MqttOverWebsocketServerInitializer;
import org.example.mqtt.broker.node.DefaultBroker;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrap {

    @SneakyThrows
    public static void main(String[] args) {
        Authenticator authenticator = packet -> 0x00;
        final Broker broker = new DefaultBroker();
        Supplier<DefaultServerSessionHandler> handlerSupplier = () ->
                new DefaultServerSessionHandler(broker, authenticator, 3);
        startServer(handlerSupplier);
    }

    public static Map<String, String> startServer(Supplier<DefaultServerSessionHandler> handlerSupplier) throws URISyntaxException,
            SSLException {
        Map<String, String> protocolToUrl = new HashMap<>(8);
        /**
         * ["mqtt://host:port", "mqtts://host:port", "ws://host:port", "wss://host:port"]
         */
        String addressArray = System.getProperty("mqtt.server.listened");
        log.info("mqtt.server.listened: {}", addressArray);
        String[] addressList = addressArray.split(",");
        for (String address : addressList) {
            URI uri = new URI(address.trim());
            InetSocketAddress bindAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
            switch (uri.getScheme()) {
                case "mqtt":
                    mqttServer(bindAddress, handlerSupplier);
                    protocolToUrl.put("mqtt", address);
                    break;
                case "mqtts":
                    secureMqttServer(bindAddress, handlerSupplier);
                    protocolToUrl.put("mqtts", address);
                    break;
                case "ws":
                    mqttOverWebsocket(bindAddress, handlerSupplier);
                    protocolToUrl.put("ws", address);
                    break;
                case "wss":
                    mqttOverSecureWebSocket(bindAddress, handlerSupplier);
                    protocolToUrl.put("wss", address);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported Schema: " + uri.getScheme());
            }
        }
        return protocolToUrl;
    }

    private static void mqttOverSecureWebSocket(InetSocketAddress address, Supplier<DefaultServerSessionHandler> handlerSupplier) throws SSLException {
        // 配置 websocket tls bootstrap
        int cpuNum = Runtime.getRuntime().availableProcessors();
        DefaultThreadFactory workerTF = new DefaultThreadFactory("netty-worker-mqttOverSecureWebsocket");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, workerTF);
        DefaultThreadFactory bossTF = new DefaultThreadFactory("netty-boss-mqttOverSecureWebsocket");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(4, bossTF);
        String certPath = System.getProperty("mqtt.server.ssl.cert", "cert/netty.zhanfengzhang.top.pem");
        String keyPath = System.getProperty("mqtt.server.ssl.key", "cert/netty.zhanfengzhang.top.pkcs8.key");
        SslContext sslCtx = SslContextBuilder.forServer(
                        ClassLoader.getSystemResourceAsStream(certPath),
                        ClassLoader.getSystemResourceAsStream(keyPath))
                .build();
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new MqttOverSecureWebsocketServerInitializer("/mqtt", sslCtx, handlerSupplier))
                .bind(address)
                .addListener(f -> log.info("MQTT over Websocket(TLS) server listened at {}", address))
                .channel().closeFuture().addListener(f -> {
                    log.info("MQTT over Websocket(TLS) server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
    }

    private static void mqttOverWebsocket(InetSocketAddress address, Supplier<DefaultServerSessionHandler> handlerSupplier) {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        DefaultThreadFactory bossTF = new DefaultThreadFactory("netty-boss-mqttOverWebsocket");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(4, bossTF);
        DefaultThreadFactory workerTF = new DefaultThreadFactory("netty-worker-mqttOverWebSocket");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, workerTF);
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new MqttOverWebsocketServerInitializer("/mqtt", handlerSupplier))
                .bind(address).addListener(f -> log.info("MQTT over Websocket server listened at {}", address))
                .channel().closeFuture().addListener(f -> {
                    log.info("MQTT over Websocket server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
    }

    private static void secureMqttServer(InetSocketAddress address,
                                         Supplier<DefaultServerSessionHandler> handlerSupplier) throws SSLException {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        DefaultThreadFactory workerTF = new DefaultThreadFactory("netty-worker-secureMqtt");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, workerTF);
        DefaultThreadFactory bossTF = new DefaultThreadFactory("netty-boss-secureMqtt");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(4, bossTF);
        String certPath = System.getProperty("mqtt.server.ssl.cert", "cert/netty.zhanfengzhang.top.pem");
        String keyPath = System.getProperty("mqtt.server.ssl.key", "cert/netty.zhanfengzhang.top.pkcs8.key");
        SslContext sslCtx = SslContextBuilder.forServer(
                        ClassLoader.getSystemResourceAsStream(certPath),
                        ClassLoader.getSystemResourceAsStream(keyPath))
                .build();
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new SecureMqttServerInitializer(sslCtx, handlerSupplier))
                .bind(address).addListener(f -> log.info("MQTT TLS server listened at {}", address))
                .channel().closeFuture().addListener(f -> {
                    log.info("MQTT TLS server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
    }

    private static void mqttServer(InetSocketAddress address, Supplier<DefaultServerSessionHandler> handlerSupplier) {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        DefaultThreadFactory bossTF = new DefaultThreadFactory("netty-boss-mqttServer");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(4, bossTF);
        DefaultThreadFactory workerTF = new DefaultThreadFactory("netty-worker-mqttServer");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, workerTF);
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new MqttServerInitializer(handlerSupplier))
                .bind(address).addListener(f -> log.info("MQTT server listened at {}", address))
                .channel().closeFuture().addListener(f -> {
                    log.info("MQTT server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
    }

}
