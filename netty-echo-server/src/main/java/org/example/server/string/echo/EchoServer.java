package org.example.server.string.echo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/02
 */
@Slf4j
public class EchoServer {

    @SneakyThrows
    public static void main(String[] args) {
        final int port = 8888;
        final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
        final ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(nioEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new EchoChannelCodec())
                                .addLast(new EchoChannelHandler());
                    }
                });
        try {
            final Channel serverChannel = serverBootstrap.bind(port).sync().channel();
            log.info("server listened at {}", port);
            serverChannel.closeFuture().sync();
        } finally {
            nioEventLoopGroup.shutdownGracefully();
        }
    }

}
