package org.example.mqtt.client;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static org.example.mqtt.model.ControlPacket.*;

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
    public void send(ControlPacket packet) {
        if (packet instanceof Publish) {
            /**
             * {@link ClientSessionImpl#publishPacketSentComplete(ControlPacketContext)}
             * will release the content
             */
            ((Publish) packet).payload().retain();
        }
        super.send(packet);
    }

    @Override
    protected void publishPacketSentComplete(ControlPacketContext cpx) {
        Publish packet = cpx.packet();
        client.ackPackets(packet.packetIdentifier(), null);
        /**
         * release {@link ClientSessionImpl#send(ControlPacket)}
         */
        packet.payload().release();
        super.publishPacketSentComplete(cpx);
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

}
