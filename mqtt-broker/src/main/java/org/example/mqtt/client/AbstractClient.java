package org.example.mqtt.client;

import static org.example.mqtt.client.ClientSessionHandler.HANDLER_NAME;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.codec.MqttCodec;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Disconnect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.SubAck;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.UnsubAck;
import org.example.mqtt.model.Unsubscribe;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2024-11-13
 */
@Slf4j
public abstract class AbstractClient implements Client {

    private static final short DEFAULT_KEEP_ALIVE = 64;

    private final String clientIdentifier;
    private final URI remoteAddress;
    private final EventLoopGroup eventLoopGroup;
    private final boolean exclusiveEventLoop;

    private final ConcurrentMap<Short, CompletableFuture<Object>> unAckPackets = new ConcurrentHashMap<>();

    private volatile CompletableFuture<ConnAck> connAckFuture;
    private volatile ClientSession session;

    protected AbstractClient(String clientIdentifier, URI remoteAddress, EventLoopGroup eventLoopGroup) {
        this.clientIdentifier = clientIdentifier;
        this.remoteAddress = remoteAddress;
        if (eventLoopGroup == null) {
            this.eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory(clientIdentifier));
            this.exclusiveEventLoop = true;
        }
        else {
            this.eventLoopGroup = eventLoopGroup;
            this.exclusiveEventLoop = false;
        }
    }

    @Override
    public String clientIdentifier() {
        return clientIdentifier;
    }

    @Override
    public CompletableFuture<ConnAck> connect(Connect connect) {
        this.connAckFuture = new CompletableFuture<>();
        ChannelFutureListener connectSuccessListener = f -> {
            if (f.isSuccess()) {// 连接成功后发送 Connect Packet
                Channel channel = f.channel();
                log.debug("Client({}) Channel connected to remote broker -> {}", clientIdentifier, channel);
                // bind Channel with Session
                this.session = new ClientSessionImpl(this, connect.cleanSession(), channel);
                channel.pipeline()
                    .addLast(new MqttCodec())
                    .addLast(HANDLER_NAME, new ClientSessionHandler(session));
                session.send(connect);
            }
            else {
                log.debug("Client({}) Channel connect to remote broker failed", clientIdentifier, f.cause());
                this.connAckFuture.completeExceptionally(f.cause());
            }
        };
        new Bootstrap()
            .group(this.eventLoopGroup).channel(NioSocketChannel.class)
            .connect(new InetSocketAddress(remoteAddress.getHost(), remoteAddress.getPort()))
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
            .addListener(connectSuccessListener)
            .channel().closeFuture().addListener(f -> {
                log.debug("Client({}) Channel was closed.", clientIdentifier);
            });
        return this.connAckFuture;
    }

    @Override
    public CompletionStage<SubAck> subscribe(List<Subscribe.Subscription> sub) {
        log.debug("Client({}) subscribe: {}", clientIdentifier(), sub);
        if (sub == null || sub.isEmpty()) {
            throw new IllegalArgumentException();
        }
        short packetIdentifier = session.nextPacketIdentifier();
        session.send(Subscribe.from(packetIdentifier, sub));
        return (CompletionStage<SubAck>) unAckPackets(packetIdentifier);
    }

    protected CompletionStage<SubAck> subscribe(String topicFilter, int qos) {
        List<Subscribe.Subscription> sub = List.of(new Subscribe.Subscription(topicFilter, qos));
        log.debug("Client try to subscribe -> client: {}, Topic: {}", clientIdentifier(), sub);
        CompletionStage<SubAck> future = subscribe(sub);
        future.whenComplete((SubAck subAck, Throwable t) -> {
            if (t != null) {
                log.warn("Client subscribe Topic failed -> client: {}", clientIdentifier(), t);
            }
            if (subAck != null) {
                log.debug("Client subscribe Topic succeed -> client: {}, Topic: {}", clientIdentifier(), subAck);
            }
        });
        return future;
    }

    @Override
    public CompletionStage<UnsubAck> unsubscribe(List<Subscribe.Subscription> unsub) {
        log.debug("Client({}) unsubscribe: {}", clientIdentifier(), unsub);
        if (unsub == null || unsub.isEmpty()) {
            throw new IllegalArgumentException();
        }
        short packetIdentifier = session.nextPacketIdentifier();
        session.send(Unsubscribe.from(packetIdentifier, unsub));
        return (CompletionStage<UnsubAck>) unAckPackets(packetIdentifier);
    }

    @Override
    public void disconnect() {
        session.send(new Disconnect());
        close();
    }

    @Override
    public CompletionStage<Void> publish(int qos, String topicName, ByteBuf payload) {
        if (qos == Publish.AT_MOST_ONCE) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            session.send(Publish.outgoing(false, qos, false, topicName, (short) 0, payload))
                .addListener(sendResultListener(future))
            ;
            // no need to wait
            return future;
        }
        else {
            short packetIdentifier = session.nextPacketIdentifier();
            final CompletableFuture<Void> future = (CompletableFuture<Void>) unAckPackets(packetIdentifier);
            session.send(Publish.outgoing(false, qos, false, topicName, packetIdentifier, payload))
                .addListener(f -> {
                    if (!f.isSuccess()) {
                        ackPacketsExceptionally(packetIdentifier, f.cause() == null ? new CancellationException() : f.cause());
                    }
                });
            return future;
        }
    }

    private GenericFutureListener<ChannelFuture> sendResultListener(CompletableFuture<Void> future) {
        return (ChannelFuture cf) -> {
            if (cf.isSuccess()) {
                future.complete(null);
            }
            else {
                future.completeExceptionally(cf.cause());
            }
        };
    }

    @Override
    public void onPublish(Publish publish) {
        // todo
        log.debug("Client({}) onPublish: {}", clientIdentifier(), publish);
    }

    private CompletableFuture<?> unAckPackets(short packetIdentifier) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        if (unAckPackets.putIfAbsent(packetIdentifier, future) != null) {
            throw new IllegalStateException();
        }
        return future;
    }

    public void ackPackets(short packetIdentifier, Object result) {
        CompletableFuture<Object> cf = unAckPackets.remove(packetIdentifier);
        if (cf != null) {
            cf.complete(result);
        }
    }

    public void ackPacketsExceptionally(short packetIdentifier, Throwable result) {
        CompletableFuture<Object> cf = unAckPackets.remove(packetIdentifier);
        if (cf != null) {
            cf.completeExceptionally(result);
        }
    }

    public void connAck(ConnAck packet) {
        this.connAckFuture.complete(packet);
    }

    @Override
    public void close() {
        if (exclusiveEventLoop) {
            eventLoopGroup.shutdownGracefully();
        }
    }

    @Override
    public short keepAlive() {
        return DEFAULT_KEEP_ALIVE;
    }

}
