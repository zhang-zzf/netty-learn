package org.github.zzf.mqtt.mqtt.client;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.CONNACK;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.SUBACK;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.UNSUBACK;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.ConnAck;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.SubAck;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.UnsubAck;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;
import org.github.zzf.mqtt.protocol.session.AbstractSession;
import org.github.zzf.mqtt.protocol.session.ControlPacketContext;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-11
 */
@Slf4j
public class ClientSessionImpl extends AbstractSession implements ClientSession {

    private final Set<Subscribe.Subscription> subscriptions = new HashSet<>();
    private final Queue<ControlPacketContext> inQueue = new LinkedList<>();
    private final Queue<ControlPacketContext> outQueue = new LinkedList<>();

    private final ConcurrentMap<Short, Unsubscribe> UNSUBSCRIBE_MAP = new ConcurrentHashMap<>();

    private final AbstractClient client;

    public ClientSessionImpl(AbstractClient client, boolean cleanSession, Channel channel) {
        super(client.clientIdentifier(), cleanSession, channel);
        this.client = client;
    }

    @Override
    public ChannelFuture send(ControlPacket packet) {
        if (!isActive()) {
            log.info("Client({}) publish failed, Session is not active. ", clientIdentifier());
            return channel().newFailedFuture(new IllegalStateException("Session is not active."));
        }
        if (packet instanceof Unsubscribe unsubscribe) {
            UNSUBSCRIBE_MAP.put(unsubscribe.packetIdentifier(), unsubscribe);
        }
        return super.send(packet).addListener((GenericFutureListener<? extends Future<? super Void>>) (f) -> {
            if (!f.isSuccess()) {
                // todo
                // close();
            }
        });
    }


    @Override
    protected void publishSentComplete(Publish packet) {
        // invoke callback after the Publish was completely sent.
        client.ackPackets(packet.packetIdentifier(), null);
        super.publishSentComplete(packet);
    }

    @Override
    public void onPacket(ControlPacket packet) {
        switch (packet.type()) {
            case CONNACK:
                doReceiveConnAck((ConnAck) packet);
                break;
            case SUBACK:
                doReceiveSubAck((SubAck) packet);
                break;
            case UNSUBACK:
                doReceiveUnsubAck((UnsubAck) packet);
                break;
            default:
                super.onPacket(packet);
        }
    }

    private void doReceiveConnAck(ConnAck packet) {
        client.connAck(packet);
    }

    private void doReceiveSubAck(SubAck packet) {
        subscriptions.addAll(packet.subscriptions());
        client.ackPackets(packet.packetIdentifier(), packet);
    }

    private void doReceiveUnsubAck(UnsubAck packet) {
        subscriptions.addAll(UNSUBSCRIBE_MAP.get(packet.packetIdentifier()).subscriptions());
        client.ackPackets(packet.packetIdentifier(), packet);
    }

    @Override
    protected void onPublish(Publish packet) {
        client.onPublish(packet);
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
    public Set<Subscribe.Subscription> subscriptions() {
        return Collections.unmodifiableSet(subscriptions);
    }

    @Override
    public int keepAlive() {
        return client.keepAlive();
    }

    private volatile boolean closing = false;

    // todo
    // @Override
    // public void close() {
    //     log.debug("Client({}) try to close session", client.clientIdentifier());
    //     if (closing) {
    //         return;
    //     }
    //     closing = true;
    //     client.close();
    //     super.close();
    // }

}
