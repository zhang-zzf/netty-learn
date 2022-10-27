package org.example.mqtt.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
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

import static org.example.mqtt.client.ClientSessionHandler.HANDLER_NAME;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/9/14
 */
@Slf4j
public class Client implements AutoCloseable {

    private final ConcurrentMap<Short, CompletableFutureWrapper<Object>> requestMap = new ConcurrentHashMap<>();
    private final String clientIdentifier;
    private final ClientSession session;
    /**
     * mqtt://host:port
     */
    private final String remoteAddress;
    public static final short KEEP_ALIVE = 8;

    private volatile ChannelPromise connAck;

    private final MessageHandler handler;

    // 3s
    private final int timeout = 3;

    public Client(String clientIdentifier, String remoteAddress, MessageHandler handler) {
        this(clientIdentifier, remoteAddress, eventLoopGroup(clientIdentifier), handler);
    }

    private static NioEventLoopGroup eventLoopGroup(String clientIdentifier) {
        return new NioEventLoopGroup(1, new DefaultThreadFactory(clientIdentifier));
    }

    @SneakyThrows
    public Client(String clientIdentifier, String remoteAddress, EventLoopGroup eventLoopGroup, MessageHandler handler) {
        this.clientIdentifier = clientIdentifier;
        this.remoteAddress = remoteAddress;
        this.session = new DefaultClientSession(this);
        this.handler = handler;
        connectToBroker(this.remoteAddress, eventLoopGroup);
    }

    public String clientIdentifier() {
        return clientIdentifier;
    }

    public void send(int qos, String topicName, ByteBuf payload)
            throws ExecutionException, InterruptedException, TimeoutException {
        sendAsync(qos, topicName, payload).get(timeout, TimeUnit.SECONDS);
    }

    public CompletableFuture<Void> sendAsync(int qos, String topicName, ByteBuf payload) {
        if (qos == Publish.AT_MOST_ONCE) {
            session.send(Publish.outgoing(false, qos, false, topicName, (short) 0, payload));
            // no need to wait
            return CompletableFuture.completedFuture(null);
        } else {
            short packetIdentifier = session.nextPacketIdentifier();
            session.send(Publish.outgoing(false, qos, false, topicName, packetIdentifier, payload));
            return cacheRequest(packetIdentifier);
        }
    }

    public List<Subscribe.Subscription> subscribe(List<Subscribe.Subscription> sub)
            throws ExecutionException, InterruptedException, TimeoutException {
        SubAck subAck = subscribeAsync(sub).get(timeout, TimeUnit.SECONDS);
        log.debug("Client({}) subAck: {}", cId(), subAck);
        return subAck.subscriptions();
    }

    public CompletableFuture<SubAck> subscribeAsync(List<Subscribe.Subscription> sub) {
        log.debug("Client({}) subscribe: {}", cId(), sub);
        if (sub == null || sub.isEmpty()) {
            throw new IllegalArgumentException();
        }
        short packetIdentifier = session.nextPacketIdentifier();
        Subscribe packet = Subscribe.from(sub).packetIdentifier(packetIdentifier);
        session.send(packet);
        return cacheRequest(packetIdentifier);
    }

    private <T> CompletableFuture<T> cacheRequest(short packetIdentifier) {
        CompletableFutureWrapper future = new CompletableFutureWrapper<>(packetIdentifier);
        if (requestMap.putIfAbsent(packetIdentifier, future) != null) {
            throw new IllegalStateException();
        }
        return future.cf;
    }

    private String cId() {
        return clientIdentifier + "-->" + remoteAddress;
    }

    public CompletableFuture<UnsubAck> unsubscribeAsync(List<Subscribe.Subscription> unsub) {
        log.debug("Client({}) unsubscribe: {}", cId(), unsub);
        if (unsub == null || unsub.isEmpty()) {
            throw new IllegalArgumentException();
        }
        short packetIdentifier = session.nextPacketIdentifier();
        Unsubscribe packet = Unsubscribe.from(unsub).packetIdentifier(packetIdentifier);
        session.send(packet);
        return cacheRequest(packetIdentifier);
    }

    @Override
    public void close() {
        session.close();
    }

    private void connectToBroker(String remoteAddress, EventLoopGroup eventLoopGroup) {
        try {
            Bootstrap bootstrap = bootstrap(eventLoopGroup, session);
            URI uri = new URI(remoteAddress);
            InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
            ChannelFuture future = bootstrap.connect(address);
            // bind Channel with Session
            final Channel channel = future.channel();
            session.bind(channel);
            // just wait 1 seconds
            if (!future.await(1, TimeUnit.SECONDS)) {
                throw new TimeoutException("Client.connectToBroker() timeout.");
            }
            if (!channel.isActive()) {
                throw new IllegalStateException("Client.connectToBroker channel is not ACTIVE.");
            }
            log.debug("Client({}) Channel connected to remote broker", cId());
            mqttProtocolConnect();
            channel.closeFuture().addListener(f -> {
                log.debug("Client({}) Channel was closed.", cId());
            });
        } catch (Exception e) {
            log.info("Client(" + cId() + ") Channel connect to remote broker exception. ", e);
            close();
            throw new RuntimeException(e);
        }
    }

    private void mqttProtocolConnect() throws InterruptedException, TimeoutException {
        log.debug("Client({}) try send Connect", cId());
        // send Connect
        connAck = session.channel().newPromise();
        session.send(Connect.from(clientIdentifier, KEEP_ALIVE));
        // careful: can not use sync to wait forever.
        // wait for 3 seconds
        boolean connected = connAck.await(timeout, TimeUnit.SECONDS);
        if (!connected) {
            throw new TimeoutException("ConnAck timeout after 3s");
        }
        log.debug("Client({}) received ConnAck", cId());
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
        CompletableFutureWrapper<Object> future = requestMap.remove(Short.valueOf(packetIdentifier));
        if (future != null) {
            future.complete(packet);
        } else {
            log.warn("Client receive *Ack [没有对应的请求]-> cId: {}, pId: {}, packet: {}", cId(), packetIdentifier, packet);
        }
    }

    public void connAck(ConnAck packet) {
        log.debug("Client({}) receive ConnAck->{}", cId(), packet);
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
        handler.handle(packet.topicName(), packet);
    }

    /**
     * Client 连接断开回调
     */
    public void disconnected() {
        log.debug("Client disconnected from remote Broker-> {}", cId());
        handler.clientClosed();
    }

    public class CompletableFutureWrapper<T> {

        final CompletableFuture<T> cf = new CompletableFuture<>();
        private final Future timeoutTask;

        public CompletableFutureWrapper(short id) {
            Runnable task = () -> {
                if (requestMap.remove(Short.valueOf(id), cf)) {
                    log.warn("Client request timeout-> cId: {}, pId: {}", cId(), id);
                    cf.completeExceptionally(new TimeoutException());
                }
            };
            this.timeoutTask = clientBoundEventLoop().schedule(task, timeout, TimeUnit.SECONDS);
        }

        public boolean complete(T value) {
            timeoutTask.cancel(true);
            return cf.complete(value);
        }

    }

    private EventLoop clientBoundEventLoop() {
        return session.channel().eventLoop();
    }

}
