package org.example.mqtt.broker.metrics;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSessionHandler;
import org.example.mqtt.broker.jvm.DefaultBroker;
import org.example.mqtt.bootstrap.MqttCodec;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrapWithMetrics {

    public static void main(String[] args) {
        final int port = 1883;
        int cpuNum = Runtime.getRuntime().availableProcessors();
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, (Runnable r) -> new Thread(r, "netty-worker"));
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(2, (Runnable r) -> new Thread(r, "netty-boss"));
        final Broker broker = new DefaultBroker();
        // 配置 bootstrap
        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ServerSessionHandler sessionHandler = new ServerSessionHandlerWithMetrics(broker, packet -> 0x00, 3);
                        ch.pipeline().addLast(new MqttCodec()).addLast(ServerSessionHandler.HANDLER_NAME, sessionHandler);
                    }
                })
                .bind(port)
                .addListener((ChannelFutureListener) future -> {
                    log.info("server listened at {}", port);
                })
                .channel().closeFuture().addListener((ChannelFutureListener) future -> {
                    log.info("server was shutdown.");
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                })
        ;
    }

}
