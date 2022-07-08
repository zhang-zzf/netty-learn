package org.example.mqtt.broker;

import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static org.example.mqtt.model.ControlPacket.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public class DefaultServerSession extends AbstractSession implements ServerSession {

    private Broker broker;
    private volatile boolean registered;
    private List<Subscription> subscriptions = new ArrayList<>();

    public DefaultServerSession(String clientIdentifier) {
        super(clientIdentifier);
    }

    @Override
    public void messageReceived(ControlPacket packet) {
        log.debug("messageReceived: {}, {}", clientIdentifier(), packet);
        switch (packet.type()) {
            case SUBSCRIBE:
                doReceiveSubscribe((Subscribe) packet);
                break;
            case UNSUBSCRIBE:
                doReceiveUnsubscribe((Unsubscribe) packet);
                break;
            case DISCONNECT:
                doReceiveDisconnect((Disconnect) packet);
                break;
            default:
                super.messageReceived(packet);
        }
    }

    @Override
    public List<Subscription> subscriptions() {
        return subscriptions;
    }

    @Override
    public Future<Void> send(ControlPacket packet) {
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        if (packet.type() == PUBLISH) {
            Publish publish = (Publish) packet;
            /**
             * {@link DefaultServerSession#publishSent(ControlPacketContext)}  will release the payload
             */
            publish.payload().retain();
            return sendInEventLoop(publish);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    protected void publishSent(ControlPacketContext cpx) {
        /**
         * release the payload retained by {@link DefaultServerSession#send(ControlPacket)}
         */
        cpx.packet().payload().release();
        super.publishSent(cpx);
    }

    @Override
    protected boolean onPublish(Publish packet, Future<Void> promise) {
        broker.onward(packet);
        return true;
    }

    private void doReceiveUnsubscribe(Unsubscribe packet) {
        log.info("doReceiveUnsubscribe req: {}, {}", clientIdentifier(), packet);
        List<Subscription> subscriptions = packet.subscriptions().stream()
                .map(s -> Subscription.from(s.topicFilter(), s.qos(), this))
                .collect(toList());
        broker().deregister(subscriptions);
        UnsubAck unsubAck = UnsubAck.from(packet.packetIdentifier());
        log.info("client({}) doReceiveUnsubscribe resp: {}", clientIdentifier(), unsubAck);
        channel().writeAndFlush(unsubAck);
    }

    private void doReceiveSubscribe(Subscribe packet) {
        log.info("client({}) doReceiveSubscribe req: {}", clientIdentifier(), packet);
        List<Subscription> subscriptions = packet.subscriptions().stream()
                .map(s -> Subscription.from(s.topicFilter(), s.qos(), this))
                .collect(toList());
        // register the Subscribe
        List<Subscription> permitted = broker().register(subscriptions);
        List<Subscribe.Subscription> permittedSubscriptions = permitted.stream()
                .map(s -> new Subscribe.Subscription(s.topicFilter(), s.qos()))
                .collect(toList());
        SubAck subAck = SubAck.from(packet.packetIdentifier(), permittedSubscriptions);
        log.info("client({}) doReceiveSubscribe resp: {}", clientIdentifier(), subAck);
        channel().writeAndFlush(subAck);
        doUpdateSessionSubscriptions(permitted);
    }

    private void doUpdateSessionSubscriptions(List<Subscription> permitted) {
        for (Subscription p : permitted) {
            boolean exists = false;
            for (Subscription sub : this.subscriptions) {
                if (sub.topicFilter().endsWith(p.topicFilter())) {
                    sub.qos(p.qos());
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                subscriptions.add(p);
            }
        }
    }

    protected void doReceiveDisconnect(Disconnect packet) {
        log.info("doReceiveDisconnect resp: {}", clientIdentifier());
        this.close();
    }

    @Override
    public Broker broker() {
        return this.broker;
    }

    @Override
    public void register(Broker broker) {
        this.broker = broker;
        broker().connect(this);
        this.registered = true;
    }

    @Override
    public boolean isRegistered() {
        return this.registered;
    }

    @Override
    public void close() {
        if (cleanSession()) {
            // disconnect the session from the broker
            broker().disconnect(this);
            this.registered = false;
        }
        super.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"registered\":").append(registered).append(',');
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        sb.append("\"cleanSession\":").append(cleanSession()).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
