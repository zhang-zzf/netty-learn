package org.example.mqtt.test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.DefaultServerSession;
import org.example.mqtt.broker.ServerSessionHandler;
import org.example.mqtt.broker.jvm.DefaultBroker;
import org.example.mqtt.codec.Codec;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrapForTest {

    public static void main(String[] args) throws InterruptedException {
        final int port = 1883;
        int cpuNum = Runtime.getRuntime().availableProcessors();
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup(cpuNum, (Runnable r) -> new Thread(r, "netty-worker"));
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(2, (Runnable r) -> new Thread(r, "netty-boss"));
        final Broker broker = new BrokerForTest();
        // 配置 bootstrap
        final ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ServerSessionHandler sessionHandler = new ServerSessionHandlerForTest(broker, packet -> 0x00, 3);
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

    public static class BrokerForTest extends DefaultBroker {

        @Override
        public void onward(Publish packet) {
            // doNothing
        }

    }

    public static class ServerSessionHandlerForTest extends ServerSessionHandler {

        public ServerSessionHandlerForTest(Broker broker, Authenticator authenticator, int activeIdleTimeoutSecond) {
            super(broker, authenticator, activeIdleTimeoutSecond);
        }

        @Override
        protected DefaultServerSession newServerSession(Connect connect) {
            return new DefaultServerSession(connect.clientIdentifier()) {
                @Override
                protected boolean onPublish(Publish packet, Future<Void> promise) {
                    // use a shadow copy of the origin Publish
                    Publish outgoing = Publish.outgoing(packet, false, "test/topic",
                            (byte) packet.qos(), nextPacketIdentifier());
                    outgoing.payload().retain();
                    sendInEventLoop(outgoing);
                    return true;
                }
            };
        }

    }

}
