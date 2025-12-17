package org.github.zzf.mqtt.mqtt.client.bootstrap;

import static java.util.Collections.singletonList;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.mqtt.broker.codec.MqttCodec;
import org.github.zzf.mqtt.protocol.model.ConnAck;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.SubAck;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.session.AbstractSession;
import org.github.zzf.mqtt.protocol.session.ControlPacketContext;
import org.github.zzf.mqtt.protocol.session.Session;

@Slf4j
public class ClientBootstrap {

    public static void main(String[] args) {
        // require args
        // mqtt://10.255.5.1:1883,mqtts://10.255.5.2:1884
        String[] remote = args[0].split(",");
        List<InetSocketAddress> remoteAddress = parseRemoteAddress(remote);
        String[] localAddr = args[1].split(":");
        InetSocketAddress local = new InetSocketAddress(localAddr[0], Integer.parseInt(localAddr[1]));
        int connections = Integer.parseInt(args[2]);
        int payloadLength = Integer.parseInt(args[3]);
        Integer sendQos = Integer.valueOf(args[4]);
        Integer topicQos = Integer.valueOf(args[5]);
        Integer period = Integer.valueOf(args[6]);
        ByteBuf payload = Unpooled.copiedBuffer(new byte[payloadLength]);
        // start config bootstrap
        Integer threadNum = Integer.getInteger("mqtt.client.thread.num", Runtime.getRuntime().availableProcessors());
        log.info("ClientBootstrap thread num: {}", threadNum);
        final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(threadNum);
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
                        ChannelFuture sync = bootstrap.connect(pickOneBroker(remoteAddress), local).sync();
                        if (sync.isSuccess()) {
                            clientCnt.getAndIncrement();
                            sync.channel().closeFuture().addListener((ChannelFuture f) -> {
                                log.info("Channel closed: {}", f.channel());
                                clientCnt.getAndDecrement();
                            });
                        }
                        Thread.sleep(Long.getLong("client.startup.sleep", 2));
                    } catch (Throwable e) {
                        log.error("connect to remote server failed", e);
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

    private static InetSocketAddress pickOneBroker(List<InetSocketAddress> remoteAddress) {
        return remoteAddress.get(new Random().nextInt(remoteAddress.size()));
    }

    private static List<InetSocketAddress> parseRemoteAddress(String[] remote) {
        return Arrays.stream(remote)
            .map(ClientBootstrap::toSocketAddress).filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    @SneakyThrows
    private static InetSocketAddress toSocketAddress(String str) {
        URI uri = new URI(str.trim());
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }

    public static class ClientTestSession extends AbstractSession {

        private final Queue<ControlPacketContext> inQueue = new LinkedList<>();
        private final Queue<ControlPacketContext> outQueue = new LinkedList<>();


        final Counter counter = Metrics.counter("com.github.zzf.netty.client.msg.out");

        final Timer publishReceived = Timer.builder("com.github.zzf.netty.client.msg")
            .tag("type", "received")
            .publishPercentileHistogram()
            // 1μs
            .minimumExpectedValue(Duration.ofNanos(1))
            .maximumExpectedValue(Duration.ofSeconds(8))
            .register(Metrics.globalRegistry);
        final Timer onPublish = Timer.builder("com.github.zzf.netty.client.msg")
            .tag("type", "in")
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofNanos(1))
            .maximumExpectedValue(Duration.ofSeconds(10))
            .register(Metrics.globalRegistry);

        protected ClientTestSession(String clientIdentifier, Channel channel) {
            super(clientIdentifier, true, channel);
        }

        @Override
        public void onPublish(Publish packet) {
            ByteBuf payload = packet.payload();
            // retain the payload that will be release by publishReceived() method
            payload.retain();
            long timeInNano = payload.getLong(0);
            long useTime = System.nanoTime() - timeInNano;
            onPublish.record(useTime, TimeUnit.NANOSECONDS);
            log.debug("client sent -> server handle -> client onPublish: {}ns", useTime);
        }

        @Override
        protected Queue<ControlPacketContext> inQueue() {
            return inQueue;
        }

        @Override
        protected Queue<ControlPacketContext> outQueue() {
            return outQueue;
        }

        @Override
        protected void publishReceivedComplete(Publish cpx) {
            ByteBuf payload = cpx.payload();
            long timeInNano = payload.getLong(0);
            long useTimeInNano = System.nanoTime() - timeInNano;
            publishReceived.record(useTimeInNano, TimeUnit.NANOSECONDS);
            log.debug("client sent -> server handle -> client publishReceived: {}ns", useTimeInNano);
            // release the payload that retained by onPublish
            payload.release();
            super.publishReceivedComplete(cpx);
        }

        @Override
        public ChannelPromise send(ControlPacket packet) {
            counter.increment();
            super.send(packet);
            return null;
        }

        @Override
        public Set<Subscribe.Subscription> subscriptions() {
            return null;
        }

    }

    public static class ClientSessionHandler extends ChannelInboundHandlerAdapter {

        // ms
        final int period;
        final ByteBuf payload;
        final byte sendQos;
        final byte topicQos;

        Session session;
        String listenedTopicFilter;

        ScheduledFuture<?> publishSendTask;

        public ClientSessionHandler(ByteBuf payload, byte sendQos, byte topicQos, int period) {
            this.payload = payload;
            this.sendQos = sendQos;
            this.topicQos = topicQos;
            this.period = period;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String[] ipAndPort = ctx.channel().localAddress().toString().substring(1).split(":");
            // client_127.0.0.1_51324
            String clientIdentifier = "client_" + ipAndPort[0] + "_" + ipAndPort[1];
            // 监听的 topic 是下一个client
            listenedTopicFilter = "client_" + ipAndPort[0] + "_" + (Integer.valueOf(ipAndPort[1]) + 1) + "/+";
            session = new ClientTestSession(clientIdentifier, ctx.channel());
            // ChannelActive send Connect
            log.info("client({}) channelActive", clientIdentifier);
            int keepAlive = period / 1000 * 4;
            keepAlive = keepAlive > Short.MAX_VALUE ? Short.MAX_VALUE : keepAlive;
            session.send(Connect.from(session.clientIdentifier(), (short) keepAlive));
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ControlPacket) {
                ControlPacket cp = (ControlPacket) msg;
                channelRead0(ctx, cp);
                // io.netty.channel.DefaultChannelPipeline.TailContext#channelRead
                // will release the ByteBuf retained from {@link MqttCodec#decode(ChannelHandlerContext, ByteBuf, List)}
                ctx.fireChannelRead(cp);
            }
            else {
                super.channelRead(ctx, msg);
            }
        }

        private void channelRead0(ChannelHandlerContext ctx, ControlPacket cp) {
            if (cp instanceof ConnAck) {
                log.debug("receive ConnAck-> {}", cp);
                // send subscribe
                // clientIdentifier -> client_127.0.0.1/51324
                // topicFilters -> client_127.0.0.1/51325
                ctx.writeAndFlush(Subscribe.from(session.nextPacketIdentifier(),
                    singletonList(new Subscribe.Subscription(listenedTopicFilter, topicQos))));
            }
            else if (cp instanceof SubAck) {
                // 开启定时任务发送 Publish
                publishSendTask = ctx.executor().scheduleAtFixedRate(() -> {
                    ByteBuf timestamp = Unpooled.buffer(16)
                        .writeLong(System.nanoTime())
                        .writeLong(System.currentTimeMillis());
                    CompositeByteBuf packet = Unpooled.compositeBuffer()
                        .addComponents(true, timestamp, this.payload);
                    // topic -> client_127.0.0.1_51322/publish
                    String topic;
                    if (Boolean.getBoolean("mqtt.client.server.only")) {
                        topic = session.clientIdentifier() + "/server/publish";
                    }
                    else {
                        topic = session.clientIdentifier() + "/publish";
                    }
                    session.send(Publish.outgoing(false, sendQos, false, topic,
                        session.nextPacketIdentifier(), packet));
                }, period, period, TimeUnit.MILLISECONDS);
            }
            else {
                session.onPacket(cp);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("Client({}) channelInactive, now close the session", session.clientIdentifier());
            if (publishSendTask != null) {
                publishSendTask.cancel(true);
            }
            session.close();
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Client({}) exceptionCaught, now close the session", session.clientIdentifier(), cause);
            session.close();
        }

    }

}
