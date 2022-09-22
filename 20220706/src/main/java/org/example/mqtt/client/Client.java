package org.example.mqtt.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.codec.MqttCodec;
import org.example.mqtt.model.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.*;

import static java.util.Collections.emptyList;
import static org.example.mqtt.client.ClientSessionHandler.HANDLER_NAME;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/9/14
 */
@Slf4j
public class Client implements AutoCloseable {

    private final ConcurrentMap<Short, SyncFuture<Object>> requestMap = new ConcurrentHashMap<>();
    private final String clientIdentifier;
    private final ClientSession session;
    /**
     * mqtt://host:port
     */
    private final String remoteAddress;
    private final EventLoopGroup eventLoop;
    public static final short KEEP_ALIVE = 120;

    private ChannelPromise connAck;

    private final MessageHandler handler;

    public Client(String clientIdentifier, String remoteAddress, MessageHandler handler) {
        this(clientIdentifier, remoteAddress, eventLoopGroup(clientIdentifier), handler);
    }

    private static NioEventLoopGroup eventLoopGroup(String clientIdentifier) {
        return new NioEventLoopGroup(1, new DefaultThreadFactory(clientIdentifier));
    }

    @SneakyThrows
    Client(String clientIdentifier, String remoteAddress, EventLoopGroup eventLoop, MessageHandler handler) {
        this.clientIdentifier = clientIdentifier;
        this.remoteAddress = remoteAddress;
        this.eventLoop = eventLoop;
        this.session = new DefaultClientSession(this);
        this.handler = handler;
        connectToBroker(this.remoteAddress);
    }

    public String clientIdentifier() {
        return clientIdentifier;
    }

    public void syncSend(byte qos, String topicName, ByteBuf payload) throws ExecutionException, InterruptedException {
        send(qos, topicName, payload).get();
    }

    public Future<Void> send(int qos, String topicName, ByteBuf payload) {
        short packetIdentifier = session.nextPacketIdentifier();
        session.send(Publish.outgoing(false, qos, false, topicName, packetIdentifier, payload));
        return cacheRequest(packetIdentifier);
    }

    public List<Subscribe.Subscription> syncSubscribe(List<Subscribe.Subscription> sub) throws InterruptedException, ExecutionException {
        return subscribe(sub).get();
    }

    public Future<List<Subscribe.Subscription>> subscribe(List<Subscribe.Subscription> sub) {
        log.info("Client({}) subscribe: {}", cId(), sub);
        if (sub == null || sub.isEmpty()) {
            return SyncFuture.completedFuture(emptyList());
        }
        short packetIdentifier = session.nextPacketIdentifier();
        Subscribe packet = Subscribe.from(sub).packetIdentifier(packetIdentifier);
        session.send(packet);
        return cacheRequest(packetIdentifier);
    }

    private SyncFuture cacheRequest(short packetIdentifier) {
        SyncFuture future = new SyncFuture<>();
        if (requestMap.putIfAbsent(packetIdentifier, future) != null) {
            throw new IllegalStateException();
        }
        return future;
    }

    private String cId() {
        return clientIdentifier + "->" + remoteAddress;
    }

    public List<Subscribe.Subscription> syncUnsubscribe(List<Subscribe.Subscription> unsub) throws ExecutionException, InterruptedException {
        return unsubscribe(unsub).get();
    }

    public Future<List<Subscribe.Subscription>> unsubscribe(List<Subscribe.Subscription> unsub) {
        log.info("Client({}) unsubscribe: {}", cId(), unsub);
        if (unsub == null || unsub.isEmpty()) {
            return SyncFuture.completedFuture(emptyList());
        }
        short packetIdentifier = session.nextPacketIdentifier();
        Unsubscribe packet = Unsubscribe.from(unsub).packetIdentifier(packetIdentifier);
        session.send(packet);
        return cacheRequest(packetIdentifier);
    }

    @Override
    public void close() {
        session.closeChannel();
        eventLoop.shutdownGracefully();
    }

    private void connectToBroker(String remoteAddress) {
        try {
            Bootstrap bootstrap = bootstrap(eventLoop, session);
            URI uri = new URI(remoteAddress);
            InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
            Channel channel = bootstrap.connect(address).sync().channel();
            log.info("Client({}) Channel connected to remote broker", cId());
            // bind Channel with Session
            session.bind(channel);
            connect(channel);
            channel.closeFuture().addListener(f -> {
                log.info("Client({}) Channel was closed.", cId());
                eventLoop.shutdownGracefully();
            });
        } catch (Exception e) {
            log.info("Client(" + cId() + ") Channel connect to remote broker exception. ", e);
            eventLoop.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    private void connect(Channel channel) throws InterruptedException {
        // send Connect
        connAck = channel.newPromise();
        session.send(Connect.from(clientIdentifier, KEEP_ALIVE));
        // wait for ConnAck
        // todo test
        connAck.sync();
    }

    private Bootstrap bootstrap(EventLoopGroup eventLoop, ClientSession clientSession) {
        final Bootstrap bootstrap = new Bootstrap()
                .group(eventLoop)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) {
                        ch.pipeline().addLast(new MqttCodec())
                                .addLast(HANDLER_NAME, new ClientSessionHandler(clientSession))
                        ;
                    }
                });
        return bootstrap;
    }

    public void completeRequest(short packetIdentifier, ControlPacket packet) {
        SyncFuture<Object> syncFuture = requestMap.get(packetIdentifier);
        if (syncFuture != null) {
            syncFuture.complete(packet);
        } else {
            log.error("Client({}) receive *Ack [没有对应的请求]: {}", cId(), packet);
        }
    }

    public void connAck(ConnAck packet) {
        if (packet.connectionAccepted()) {
            connAck.setSuccess();
        } else {
            connAck.setFailure(new RuntimeException("Broker reject Connect with " + packet));
        }
    }

    /**
     * 接受到消息
     *
     * @param packet Publish
     */
    public void receivePublish(Publish packet) {
        log.debug("receivePublish: {}", packet);
        handler.handle(packet.topicName(), ByteBufUtil.getBytes(packet.payload()));
    }

    /**
     * Client 连接断开回调
     */
    public void disconnected() {
        log.info("Client({}) disconnected from remote Broker", cId());
        handler.clientClosed();
    }

    public static class SyncFuture<V> implements Future<V> {

        private volatile V response;
        private final CountDownLatch latch = new CountDownLatch(1);

        public SyncFuture() {
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            return latch.getCount() == 0;
        }

        @Override
        public V get() throws InterruptedException {
            latch.await();
            return this.response;
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException {
            if (latch.await(timeout, unit)) {
                return this.response;
            }
            return null;
        }

        public void complete(V response) {
            this.response = response;
            latch.countDown();
        }

        public static <T> SyncFuture<T> completedFuture(T response) {
            SyncFuture<T> f = new SyncFuture<>();
            f.complete(response);
            return f;
        }

    }


}
