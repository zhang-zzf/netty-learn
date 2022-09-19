package org.example.mqtt.client;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;

import java.util.HashSet;
import java.util.Set;

import static org.example.mqtt.model.ControlPacket.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/5
 */
@Slf4j
public class DefaultClientSession extends AbstractSession implements ClientSession {

    private final Set<Subscribe.Subscription> subscriptions = new HashSet<>();

    private final Client client;

    public DefaultClientSession(Client client) {
        super(client.clientIdentifier());
        this.client = client;
    }

    @Override
    public void send(ControlPacket packet) {
        if (packet instanceof Publish) {
            /**
             * {@link DefaultClientSession#publishSendComplete(ControlPacketContext)}
             * will release the content
             */
            ((Publish) packet).payload().retain();
        } else if (packet instanceof Connect) {
        }
        super.send(packet);
    }

    @Override
    protected void publishSendComplete(ControlPacketContext cpx) {
        super.publishSendComplete(cpx);
        Publish packet = cpx.packet();
        client.completeRequest(packet.packetIdentifier(), null);
        /**
         * release {@link DefaultClientSession#send(ControlPacket)}
         */
        packet.payload().release();
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
        client.completeRequest(packet.packetIdentifier(), packet);
    }

    private void doReceiveUnsubAck(UnsubAck packet) {
        client.completeRequest(packet.packetIdentifier(), packet);
    }

    @Override
    protected boolean onPublish(Publish packet) {
        client.receivePublish(packet);
        return true;
    }

    @Override
    public Set<Subscribe.Subscription> subscriptions() {
        return subscriptions;
    }

    @Override
    public int keepAlive() {
        return client.KEEP_ALIVE;
    }

}
