package org.github.zzf.mqtt.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.codec.ControlPacketRecycler;
import org.github.zzf.mqtt.protocol.codec.MqttCodec;
import org.github.zzf.mqtt.protocol.server.Authenticator;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.RetainPublishManager;
import org.github.zzf.mqtt.protocol.server.RoutingTable;
import org.github.zzf.mqtt.protocol.server.TopicBlocker;
import org.github.zzf.mqtt.server.codec.websocket.MqttOverSecureWebsocketServerInitializer;
import org.github.zzf.mqtt.server.codec.websocket.MqttOverWebsocketServerInitializer;

@Slf4j
@Builder
public class BrokerBootstrap {

    private final Map<String, ListenPort> LISTENED_SERVERS = new HashMap<>(8);

    Authenticator authenticator;
    RoutingTable routingTable;
    TopicBlocker topicBlocker;
    RetainPublishManager retainPublishManager;
    int workerThreadNum;
    /* mqtt://host:port,mqtts://host:port,ws://host:port,wss://host:port */
    String serverListenedAddress;
    int activeIdleTimeoutSecond;

    @SneakyThrows
    public BrokerBootstrap start() {
        // mqtt / mqtts / ws / wss use the same Broker
        Broker broker = new DefaultBroker(authenticator, routingTable, topicBlocker, retainPublishManager);
        /* ["mqtt://host:port", "mqtts://host:port", "ws://host:port", "wss://host:port"] */
        String[] addressList = serverListenedAddress.split(",");
        for (String address : addressList) {
            URI uri = new URI(address.trim());
            InetSocketAddress bindAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
            Channel channel;
            switch (uri.getScheme()) {
                case "mqtt":
                    channel = mqttServer(bindAddress, broker);
                    LISTENED_SERVERS.put("mqtt", new ListenPort(address, channel));
                    break;
                case "mqtts":
                    channel = secureMqttServer(bindAddress, broker);
                    LISTENED_SERVERS.put("mqtts", new ListenPort(address, channel));
                    break;
                case "ws":
                    channel = mqttOverWebsocket(bindAddress, broker);
                    LISTENED_SERVERS.put("ws", new ListenPort(address, channel));
                    break;
                case "wss":
                    channel = mqttOverSecureWebSocket(bindAddress, broker);
                    LISTENED_SERVERS.put("wss", new ListenPort(address, channel));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported Schema: " + uri.getScheme());
            }
        }
        // ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            closeListenedPort();
            broker.close();
        }));
        return this;
    }

    @SneakyThrows
    public void closeListenedPort() {
        // first shutdown the listened servers
        for (Map.Entry<String, ListenPort> e : LISTENED_SERVERS.entrySet()) {
            log.info("Shutdown Server... -> {}", e.getValue());
            e.getValue().getChannel().close().sync().addListener(f -> {
                if (f.isSuccess()) {
                    log.info("Server Shutdown Success: -> {}", e.getValue());
                }
                else {
                    log.error("Server Shutdown Failed: -> {}", e.getValue());
                }
            });
        }
    }


    private Channel mqttOverSecureWebSocket(InetSocketAddress address,
            Broker broker) throws SSLException {
        // 配置 websocket tls bootstrap
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(workerThreadNum, new DefaultThreadFactory("wss-worker"));
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("wss-boss"));
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
                    .childHandler(new MqttOverSecureWebsocketServerInitializer("/mqtt", sslCtx, broker, activeIdleTimeoutSecond))
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

    private Channel mqttOverWebsocket(InetSocketAddress address,
            Broker broker) {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("ws-boss"));
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(workerThreadNum, new DefaultThreadFactory("ws-worker"));
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    // 设置 Channel 类型，通过反射创建 Channel 对象
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new MqttOverWebsocketServerInitializer("/mqtt", broker, activeIdleTimeoutSecond))
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

    private Channel secureMqttServer(InetSocketAddress address,
            Broker broker) throws SSLException {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("mqtts-boss"));
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(workerThreadNum, new DefaultThreadFactory("mqtts-worker"));
        String certPath = System.getProperty("mqtt.server.ssl.cert", "cert/netty.zhanfengzhang.top.pem");
        String keyPath = System.getProperty("mqtt.server.ssl.key", "cert/netty.zhanfengzhang.top.pkcs8.key");
        final SslContext sslCtx = SslContextBuilder.forServer(
                        ClassLoader.getSystemResourceAsStream(certPath),
                        ClassLoader.getSystemResourceAsStream(keyPath))
                .build();
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    // 设置 Channel 类型，通过反射创建 Channel 对象
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(sslCtx.newHandler(ch.alloc()))
                                    .addLast(new MqttCodec())
                                    .addLast(DefaultServerSessionHandler.HANDLER_NAME, new DefaultServerSessionHandler(broker, activeIdleTimeoutSecond))
                                    .addLast(new ControlPacketRecycler())
                            ;
                        }
                    })
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

    private Channel mqttServer(InetSocketAddress address,
            Broker broker) {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("mqtt-boss", false, Thread.MAX_PRIORITY));
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(workerThreadNum, new DefaultThreadFactory("mqtt-worker"));
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    // 设置 Channel 类型，通过反射创建 Channel 对象
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new MqttCodec())
                                    .addLast(DefaultServerSessionHandler.HANDLER_NAME, new DefaultServerSessionHandler(broker, activeIdleTimeoutSecond))
                                    .addLast(new ControlPacketRecycler())
                            ;
                        }
                    })
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
