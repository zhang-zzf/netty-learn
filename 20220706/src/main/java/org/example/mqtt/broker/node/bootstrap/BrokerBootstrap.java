package org.example.mqtt.broker.node.bootstrap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrap {

    private static final Map<String, ListenPort> LISTENED_SERVER = new HashMap<>(8);
    public static final Map<String, ListenPort> LISTENED_SERVERS = Collections.unmodifiableMap(LISTENED_SERVER);
    public static final int MQTT_SERVER_THREAD_NUM;

    static {
        MQTT_SERVER_THREAD_NUM = Integer.getInteger("mqtt.server.thread.num",
                Runtime.getRuntime().availableProcessors() * 2);
        log.info("MQTT_SERVER_THREAD_NUM-> {}", MQTT_SERVER_THREAD_NUM);
    }

    @SneakyThrows
    public static void main(String[] args) {
        if (!Boolean.getBoolean("spring.enable")) {
            startServer(packet -> 0x00, new DefaultBroker());
        } else {
            // start with spring context
            log.info("BrokerBootstrap in spring context");
            BrokerBootstrapInSpringContext.main(args);
        }
    }

    public static void startServer(Authenticator authenticator, Broker broker) throws URISyntaxException, SSLException {
        Supplier<DefaultServerSessionHandler> handlerSupplier = () ->
                new DefaultServerSessionHandler(broker, authenticator, 3);
        startServer(handlerSupplier);
    }

    public static void shutdownServer() {
        for (Map.Entry<String, ListenPort> e : LISTENED_SERVERS.entrySet()) {
            e.getValue().getChannel().close();
        }
    }

    public static void startServer(Supplier<DefaultServerSessionHandler> handlerSupplier) throws URISyntaxException,
            SSLException {
        /**
         * ["mqtt://host:port", "mqtts://host:port", "ws://host:port", "wss://host:port"]
         */
        String addressArray = System.getProperty("mqtt.server.listened");
        log.info("mqtt.server.listened: {}", addressArray);
        String[] addressList = addressArray.split(",");
        for (String address : addressList) {
            URI uri = new URI(address.trim());
            InetSocketAddress bindAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
            Channel channel;
            switch (uri.getScheme()) {
                case "mqtt":
                    channel = mqttServer(bindAddress, handlerSupplier);
                    LISTENED_SERVER.put("mqtt", new ListenPort(address, channel));
                    break;
                case "mqtts":
                    channel = secureMqttServer(bindAddress, handlerSupplier);
                    LISTENED_SERVER.put("mqtts", new ListenPort(address, channel));
                    break;
                case "ws":
                    channel = mqttOverWebsocket(bindAddress, handlerSupplier);
                    LISTENED_SERVER.put("ws", new ListenPort(address, channel));
                    break;
                case "wss":
                    channel = mqttOverSecureWebSocket(bindAddress, handlerSupplier);
                    LISTENED_SERVER.put("wss", new ListenPort(address, channel));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported Schema: " + uri.getScheme());
            }
        }
    }

    private static Channel mqttOverSecureWebSocket(InetSocketAddress address,
                                                   Supplier<DefaultServerSessionHandler> handlerSupplier) throws SSLException {
        // 配置 websocket tls bootstrap
        DefaultThreadFactory workerTF = new DefaultThreadFactory("wss-worker");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(MQTT_SERVER_THREAD_NUM, workerTF);
        DefaultThreadFactory bossTF = new DefaultThreadFactory("wss-boss");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, bossTF);
        String certPath = System.getProperty("mqtt.server.ssl.cert", "cert/netty.zhanfengzhang.top.pem");
        String keyPath = System.getProperty("mqtt.server.ssl.key", "cert/netty.zhanfengzhang.top.pkcs8.key");
        SslContext sslCtx = SslContextBuilder.forServer(
                        ClassLoader.getSystemResourceAsStream(certPath),
                        ClassLoader.getSystemResourceAsStream(keyPath))
                .build();
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    // 设置 Channel 类型，通过反射创建 Channel 对象
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new MqttOverSecureWebsocketServerInitializer("/mqtt", sslCtx, handlerSupplier))
                    .bind(address).sync()
                    .addListener(f -> log.info("MQTT over Websocket(TLS) server listened at {}", address))
                    .channel().closeFuture().addListener(f -> {
                        bossGroup.shutdownGracefully();
                        workerGroup.shutdownGracefully();
                        log.info("MQTT over Websocket(TLS) server was shutdown.");
                    });
            return future.channel();
        } catch (Exception e) {
            log.info("MQTT over Websocket(TLS) server was shutdown.", e);
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    private static Channel mqttOverWebsocket(InetSocketAddress address, Supplier<DefaultServerSessionHandler> handlerSupplier) {
        DefaultThreadFactory bossTF = new DefaultThreadFactory("ws-worker");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, bossTF);
        DefaultThreadFactory workerTF = new DefaultThreadFactory("ws-boss");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(MQTT_SERVER_THREAD_NUM, workerTF);
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    // 设置 Channel 类型，通过反射创建 Channel 对象
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new MqttOverWebsocketServerInitializer("/mqtt", handlerSupplier))
                    .bind(address).sync()
                    .addListener(f -> log.info("MQTT over Websocket server listened at {}", address))
                    .channel().closeFuture().addListener(f -> {
                        log.info("MQTT over Websocket server was shutdown.");
                        bossGroup.shutdownGracefully();
                        workerGroup.shutdownGracefully();
                    });
            return future.channel();
        } catch (Exception e) {
            log.info("MQTT over Websocket server was shutdown.", e);
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    private static Channel secureMqttServer(InetSocketAddress address,
                                            Supplier<DefaultServerSessionHandler> handlerSupplier) throws SSLException {
        DefaultThreadFactory workerTF = new DefaultThreadFactory("mqtts-worker");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(MQTT_SERVER_THREAD_NUM, workerTF);
        DefaultThreadFactory bossTF = new DefaultThreadFactory("mqtts-boss");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, bossTF);
        String certPath = System.getProperty("mqtt.server.ssl.cert", "cert/netty.zhanfengzhang.top.pem");
        String keyPath = System.getProperty("mqtt.server.ssl.key", "cert/netty.zhanfengzhang.top.pkcs8.key");
        SslContext sslCtx = SslContextBuilder.forServer(
                        ClassLoader.getSystemResourceAsStream(certPath),
                        ClassLoader.getSystemResourceAsStream(keyPath))
                .build();
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    // 设置 Channel 类型，通过反射创建 Channel 对象
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new SecureMqttServerInitializer(sslCtx, handlerSupplier))
                    .bind(address).sync()
                    .addListener(f -> log.info("MQTT TLS server listened at {}", address))
                    .channel().closeFuture().addListener(f -> {
                        log.info("MQTT TLS server was shutdown.");
                        bossGroup.shutdownGracefully();
                        workerGroup.shutdownGracefully();
                    });
            return future.channel();
        } catch (Exception e) {
            log.info("MQTT TLS server was shutdown.", e);
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    private static Channel mqttServer(InetSocketAddress address,
                                      Supplier<DefaultServerSessionHandler> handlerSupplier) {
        DefaultThreadFactory bossTF = new DefaultThreadFactory("mqtt-boss");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, bossTF);
        DefaultThreadFactory workerTF = new DefaultThreadFactory("mqtt-worker");
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(MQTT_SERVER_THREAD_NUM, workerTF);
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    // 设置 Channel 类型，通过反射创建 Channel 对象
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new MqttServerInitializer(handlerSupplier))
                    .bind(address).sync().addListener(f -> log.info("MQTT server listened at {}", address))
                    .channel().closeFuture().addListener(f -> {
                        bossGroup.shutdownGracefully();
                        workerGroup.shutdownGracefully();
                        log.info("MQTT server was shutdown.");
                    });
            return future.channel();
        } catch (Exception e) {
            log.info("MQTT server was shutdown.", e);
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ListenPort {

        // mqtt://host:port
        private String url;
        private Channel channel;

    }

}
