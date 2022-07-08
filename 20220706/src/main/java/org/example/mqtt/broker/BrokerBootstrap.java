package org.example.mqtt.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.jvm.DefaultBroker;
import org.example.mqtt.codec.Codec;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrap {

    public static void main(String[] args) throws InterruptedException {
        final int port = 1883;
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup(8, (Runnable r) -> new Thread(r, "netty-worker"));
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(2, (Runnable r) -> new Thread(r, "netty-boss"));
        final Broker broker = new DefaultBroker();
        // 配置 bootstrap
        final ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ServerSessionHandler sessionHandler = new ServerSessionHandler(broker, packet -> 0x00, 3);
                        ch.pipeline()
                                .addLast(new Codec())
                                .addLast(ServerSessionHandler.HANDLER_NAME, sessionHandler);
                    }
                });
        try {
            final Channel serverChannel = serverBootstrap.bind(port).sync().channel();
            log.info("server listened at {}", port);
            serverChannel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

}
