package org.example.mqtt.client;

import static org.example.mqtt.model.ControlPacket.CONNACK;
import static org.example.mqtt.model.ControlPacket.SUBACK;
import static org.example.mqtt.model.ControlPacket.UNSUBACK;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.SubAck;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.UnsubAck;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-11
 */
@Slf4j
public class ClientSessionImpl extends AbstractSession implements ClientSession {

    private final Set<Subscribe.Subscription> subscriptions = new HashSet<>();
    private final Queue<ControlPacketContext> inQueue = new LinkedList<>();
    private final Queue<ControlPacketContext> outQueue = new LinkedList<>();

    private final AbstractClient client;


    public ClientSessionImpl(AbstractClient client, boolean cleanSession, Channel channel) {
        super(client.clientIdentifier(), cleanSession, channel);
        this.client = client;
    }

    @Override
    protected void publishPacketSentComplete(ControlPacketContext cpx) {
        super.publishPacketSentComplete(cpx);
        // invoke callback after the Publish was completely sent.
        client.ackPacketsExceptionally(cpx.packet().packetIdentifier(), null);
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
        client.ackPackets(packet.packetIdentifier(), packet);
    }

    private void doReceiveUnsubAck(UnsubAck packet) {
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
        return subscriptions;
    }

    @Override
    public int keepAlive() {
        return client.keepAlive();
    }

    @Override
    public void close() {
        client.close();
        super.close();
    }

    @Override
    public ChannelFuture send(ControlPacket packet) {
        if (!isActive()) {
            log.info("Client({}) publish failed, Session is not active. ", clientIdentifier());
            return channel().newFailedFuture(new IllegalStateException("Session is not active."));
        }
        return super.send(packet).addListener((GenericFutureListener<? extends Future<? super Void>>) (f) -> {
            if (!f.isSuccess()) {
                close();
            }
        })
            ;
    }


}
