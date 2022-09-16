package org.example.mqtt.client.bootstrap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.codec.MqttCodec;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;
import org.example.mqtt.session.Session;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
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

    public static void main(String[] args) {
        // require args
        String[] remoteAddr = args[0].split(":");
        String[] localAddr = args[1].split(":");
        InetSocketAddress remote = new InetSocketAddress(remoteAddr[0], Integer.parseInt(remoteAddr[1]));
        InetSocketAddress local = new InetSocketAddress(localAddr[0], Integer.parseInt(localAddr[1]));
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
                    protected void initChannel(NioSocketChannel ch) {
                        ch.pipeline()
                                .addLast(new MqttCodec())
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            nioEventLoopGroup.shutdownGracefully();
        }
    }

    public static class ClientTestSession extends AbstractSession {

        final Timer publishReceived = Timer.builder("com.github.zzf.netty.client.msg")
                .tag("type", "received")
                .publishPercentileHistogram()
                // 1μs
                .minimumExpectedValue(Duration.ofNanos(1000))
                .maximumExpectedValue(Duration.ofSeconds(10))
                .register(Metrics.globalRegistry);
        final Timer onPublish = Timer.builder("com.github.zzf.netty.client.msg")
                .tag("type", "in")
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(1000))
                .maximumExpectedValue(Duration.ofSeconds(10))
                .register(Metrics.globalRegistry);

        protected ClientTestSession(String clientIdentifier) {
            super(clientIdentifier);
        }

        @Override
        public boolean onPublish(Publish packet) {
            ByteBuf payload = packet.payload();
            // retain the payload that will be release by publishReceived() method
            payload.retain();
            long timeInNano = payload.getLong(0);
            long useTime = System.nanoTime() - timeInNano;
            onPublish.record(useTime, TimeUnit.NANOSECONDS);
            log.debug("client sent -> server handle -> client onPublish: {}ns", useTime);
            return true;
        }

        @Override
        protected void qoS2PublishReceived(ControlPacketContext cpx) {
            ByteBuf payload = cpx.packet().payload();
            long timeInNano = payload.getLong(0);
            long useTime = System.nanoTime() - timeInNano;
            publishReceived.record(useTime, TimeUnit.NANOSECONDS);
            log.debug("client sent -> server handle -> client publishReceived: {}ns", useTime);
            // release the payload that retained by onPublish
            payload.release();
            super.qoS2PublishReceived(cpx);
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
                ControlPacket cp = (ControlPacket) msg;
                try {
                    channelRead0(ctx, cp);
                } finally {
                    /**
                     * release the ByteBuf retained from
                     * {@link MqttCodec#decode(ChannelHandlerContext, ByteBuf, List)}
                     */
                    cp.content().release();
                }
            } else {
                super.channelRead(ctx, msg);
            }
        }

        private void channelRead0(ChannelHandlerContext ctx, ControlPacket cp) {
            if (cp instanceof ConnAck) {
                // send subscribe
                // client_127.0.0.1/51324/#
                String topicFilter = topic + "/#";
                ctx.writeAndFlush(Subscribe.from(session.nextPacketIdentifier(),
                        singletonList(new Subscribe.Subscription(topicFilter, topicQos))));
            } else if (cp instanceof SubAck) {
                // 开启定时任务发送 Publish
                publishSendTask = ctx.executor().scheduleWithFixedDelay(() -> {
                    ByteBuf timestamp = Unpooled.buffer(16)
                            .writeLong(System.nanoTime())
                            .writeLong(System.currentTimeMillis());
                    CompositeByteBuf packet = Unpooled.compositeBuffer()
                            .addComponents(true, timestamp, this.payload);
                    session.send(Publish.outgoing(false, sendQos, false, topic,
                            session.nextPacketIdentifier(), packet));
                    logOutgoingMetrics();
                }, 1, period, TimeUnit.SECONDS);
            } else {
                session.messageReceived(cp);
            }
        }

        final Counter counter = Metrics.counter("com.github.zzf.netty.client.msg", "type", "out");

        private void logOutgoingMetrics() {
            counter.increment();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("client({}) channelInactive, now close the session", session.clientIdentifier());
            if (publishSendTask != null) {
                publishSendTask.cancel(true);
            }
            session.closeChannel();
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("client({}) exceptionCaught, now close the session", session.clientIdentifier(), cause);
            session.closeChannel();
        }

    }

}
