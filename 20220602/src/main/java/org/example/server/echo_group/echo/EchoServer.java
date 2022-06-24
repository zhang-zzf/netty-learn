package org.example.server.echo_group.echo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/02
 */
@Slf4j
public class EchoServer {

    static final ChannelInboundHandlerAdapter logHandler = new ChannelInboundHandlerAdapter() {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            log.info("channelRegistered");
            super.channelRegistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("channelActive");
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            log.info("channelRead");
            super.channelRead(ctx, msg);
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            log.info("handlerAdded");
            super.handlerAdded(ctx);
        }
    };

    @SneakyThrows
    public static void main(String[] args) {
        final int port = 8888;
        final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
        // 配置 bootstrap
        final ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(nioEventLoopGroup)
                .handler(logHandler)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new DelimiterBasedFrameDecoder(256, Delimiters.lineDelimiter()))
                                .addLast(new StringDecoder(UTF_8))
                                .addLast(new StringEncoder(UTF_8))
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
