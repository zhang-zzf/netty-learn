package org.example.mqtt.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.codec.Codec;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.Session;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/06
 */
@Slf4j
public class ClientBootstrap {

    public static void main(String[] args) throws InterruptedException {
        // require args
        String[] remoteAddr = args[0].split(":");
        String[] localAddr = args[1].split(":");
        InetSocketAddress remote = new InetSocketAddress(remoteAddr[0], Integer.valueOf(remoteAddr[1]));
        InetSocketAddress local = new InetSocketAddress(localAddr[0], Integer.valueOf(localAddr[1]));
        int connections = Integer.parseInt(args[2]);
        int payloadLength = Integer.parseInt(args[3]);
        Integer sendQos = Integer.valueOf(args[4]);
        Integer topicQos = Integer.valueOf(args[5]);
        Integer period = Integer.valueOf(args[6]);
        ByteBuf payload = Unpooled.copiedBuffer(new byte[payloadLength]);
        // start config bootstrap
        final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(4);
        // 配置 bootstrap
        final Bootstrap bootstrap = new Bootstrap()
                .group(nioEventLoopGroup)
                // 设置 Channel 类型，通过反射创建 Channel 对象
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new Codec())
                                .addLast(new ClientSessionHandler(payload, sendQos.byteValue(), topicQos.byteValue(), period))
                        ;
                    }
                });
        try {
            AtomicInteger clientCnt = new AtomicInteger(0);
            while (!Thread.currentThread().isInterrupted()) {
                // 开始连接
                if (clientCnt.get() >= connections) {
                    // 10 秒 检测一次
                    Thread.sleep(10000);
                    continue;
                }
                log.info("ClientBootstrap had clients: {}", clientCnt.get());
                // 同步检测是否有 connections 个 client
                while (clientCnt.get() < connections) {
                    try {
                        ChannelFuture sync = bootstrap.connect(remote, local).sync();
                        if (sync.isSuccess()) {
                            clientCnt.getAndIncrement();
                            sync.channel().closeFuture().addListener((ChannelFuture f) -> {
                                log.info("Channel closed: {}", f.channel());
                                clientCnt.getAndDecrement();
                            });
                        }
                    } catch (Throwable e) {
                        log.debug("connect to remote server failed", e);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException interruptedException) {
                            // ignore
                        }
                    }
                }
                log.info("ClientBootstrap now has clients: {}", clientCnt.get());
            }
        } finally {
            nioEventLoopGroup.shutdownGracefully();
        }
    }

    public static class ClientTestSession extends AbstractSession {

        protected ClientTestSession(String clientIdentifier) {
            super(clientIdentifier);
        }

        @Override
        public boolean onPublish(Publish packet, Future<Void> promise) {
            return true;
        }

        @Override
        public Set<Subscribe.Subscription> subscriptions() {
            return null;
        }

    }

    public static class ClientSessionHandler extends ChannelInboundHandlerAdapter {

        final int period;
        final ByteBuf payload;
        final byte sendQos;
        final byte topicQos;

        Session session;
        String topic;

        ScheduledFuture<?> publishSendTask;

        public ClientSessionHandler(ByteBuf payload, byte sendQos, byte topicQos, int period) {
            this.payload = payload;
            this.sendQos = sendQos;
            this.topicQos = topicQos;
            this.period = period;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String clientIdentifier = ctx.channel().localAddress().toString();
            String[] ipAndPort = clientIdentifier.substring(1).split(":");
            // client_127.0.0.1/51324
            topic = "client_" + ipAndPort[0] + "/" + ipAndPort[1];
            session = new ClientTestSession(clientIdentifier);
            // bind the channel
            session.bind(ctx.channel());
            // ChannelActive send Connect
            log.info("client({}) channelActive", clientIdentifier);
            int keepAlive = period * 4;
            keepAlive = keepAlive > Short.MAX_VALUE ? Short.MAX_VALUE : keepAlive;
            session.send(Connect.from(session.clientIdentifier(), (short) keepAlive));
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ControlPacket) {
                try {
                    channelRead0(ctx, msg);
                } finally {
                    /*
                      release the ByteBuf retained from {@link Codec#decode(ChannelHandlerContext, ByteBuf, List)}
                     */
                    ((ControlPacket) msg).content().release();
                }
            } else {
                super.channelRead(ctx, msg);
            }
        }

        public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ConnAck) {
                // send subscribe
                // client_127.0.0.1/51324/#
                String topicFilter = topic + "/#";
                ctx.writeAndFlush(Subscribe.from(session.nextPacketIdentifier(),
                        singletonList(new Subscribe.Subscription(topicFilter, topicQos))));
            } else if (msg instanceof SubAck) {
                // 开启定时任务发送 Publish
                publishSendTask = ctx.executor().scheduleWithFixedDelay(() -> {
                    session.send(Publish.outgoing(false, sendQos, false, topic,
                            session.nextPacketIdentifier(), payload));
                }, 1, period, TimeUnit.SECONDS);
            } else {
                session.messageReceived((ControlPacket) msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("client({}) channelInactive, now close the session", session.clientIdentifier());
            if (publishSendTask != null) {
                publishSendTask.cancel(true);
            }
            session.close();
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("client({}) exceptionCaught, now close the session", session.clientIdentifier(), cause);
            session.close();
        }

    }

}
