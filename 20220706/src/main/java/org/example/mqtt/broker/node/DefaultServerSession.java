package org.example.mqtt.broker.node;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.example.mqtt.model.ControlPacket.*;

/**
 * Session 的生命周期由 Broker 控制
 *
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public class DefaultServerSession extends AbstractSession implements ServerSession {

    private Broker broker;
    private volatile boolean registered;
    protected Set<Subscribe.Subscription> subscriptions = new HashSet<>();
    /**
     * Will Message
     * <pre>
     *     initiate by Connect if will flag is present. It will be cleaned after
     *     1. receive Disconnect
     *     2. lost the Channel with the Client, and forward the message to relative Topic.
     * </pre>
     */
    private volatile Publish willMessage;

    public DefaultServerSession(String clientIdentifier) {
        super(clientIdentifier);
    }

    public static ServerSession from(Connect connect) {
        return new DefaultServerSession(connect.clientIdentifier())
                .reInitWith(connect);
    }

    public DefaultServerSession reInitWith(Connect connect) {
        cleanSession(connect.cleanSession());
        if (connect.willFlag()) {
            willMessage = extractWillMessage(connect);
        }
        return this;
    }

    @Override
    public void onPacket(ControlPacket packet) {
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
                super.onPacket(packet);
        }
    }

    @Override
    public Set<Subscribe.Subscription> subscriptions() {
        return subscriptions;
    }

    @Override
    public void send(ControlPacket packet) {
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        if (packet.type() == PUBLISH) {
            Publish publish = (Publish) packet;
            log.debug("sender({}/{}) [send Publish]", cId(), publish.pId());
            /**
             *  {@link DefaultServerSession#publishPacketSentComplete(ControlPacketContext)}  will release the payload
             */
            publish.payload().retain();
            log.debug("sender({}/{}) [retain Publish.payload]", cId(), publish.pId());
            super.send(publish);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    protected void publishPacketSentComplete(ControlPacketContext cpx) {
        /**
         * release the payload retained by {@link DefaultServerSession#send(ControlPacket)}
         */
        cpx.packet().payload().release();
        log.debug("sender({}/{}) [release Publish.payload]", cId(), cpx.pId());
        super.publishPacketSentComplete(cpx);
    }

    @Override
    protected boolean onPublish(Publish packet) {
        broker.handlePublish(packet);
        return true;
    }

    private void doReceiveUnsubscribe(Unsubscribe packet) {
        log.info("Session({}) doReceiveUnsubscribe req: {}", cId(), packet);
        broker.unsubscribe(this, packet);
        doRemoveSubscriptions(packet.subscriptions());
        UnsubAck unsubAck = UnsubAck.from(packet.packetIdentifier());
        log.info("Session({}) doReceiveUnsubscribe resp: {}", cId(), unsubAck);
        channel().writeAndFlush(unsubAck);
    }

    protected void doRemoveSubscriptions(List<Subscribe.Subscription> subscriptions) {
        this.subscriptions.removeAll(subscriptions);
    }

    private void doReceiveSubscribe(Subscribe packet) {
        log.debug("Session({}) doReceiveSubscribe req: {}", cId(), packet);
        // register the Subscribe
        List<Subscribe.Subscription> permitted = broker.subscribe(this, packet);
        SubAck subAck = SubAck.from(packet.packetIdentifier(), permitted);
        log.debug("Session({}) doReceiveSubscribe resp: {}", cId(), subAck);
        doAddSubscriptions(permitted);
        channel().writeAndFlush(subAck);
        doSendRetainPublish(permitted);
    }

    private void doSendRetainPublish(List<Subscribe.Subscription> permitted) {
        for (Subscribe.Subscription p : permitted) {
            for (Publish packet : broker.retainMatch(p.topicFilter())) {
                log.debug("Session({}) match retain Publish: {}", cId(), packet);
                // send retain Publish
                int qos = Math.min(packet.qos(), p.qos());
                // do rebuild the PublishPacket
                send(Publish.outgoing(packet, p.topicFilter(), (byte) qos, nextPacketIdentifier()));
            }
        }
    }

    protected void doAddSubscriptions(List<Subscribe.Subscription> permitted) {
        this.subscriptions.removeAll(permitted);
        this.subscriptions.addAll(permitted);
    }

    protected void doReceiveDisconnect(Disconnect packet) {
        log.debug("Session({}) doReceiveDisconnect.", clientIdentifier());
        // clean the Will message.
        if (willMessage != null) {
            log.debug("Session({}) Disconnect, now clear Will: {}", cId(), willMessage);
            willMessage = null;
        }
        close();
    }

    @Override
    public Broker broker() {
        return broker;
    }

    @Override
    public boolean isRegistered() {
        return registered;
    }

    @Override
    public void open(Channel ch, Broker broker) {
        log.debug("Session({}) Channel<->Session<->Broker: {}, {}", cId(), ch, broker);
        bind(ch);
        if (!registered) {
            log.debug("Session({}) now try to connect to Broker: {}", cId(), broker);
            this.broker = broker;
            broker.connect(this);
            registered = true;
            log.debug("Session({}) now connected to Broker", cId());
        }
    }

    @Override
    public void channelClosed() {
        super.channelClosed();
        // 取消与 Broker 的关联
        if (registered) {
            if (cleanSession()) {
                log.debug("Session({}) now try to disconnect from Broker: {}", cId(), broker);
                // disconnect the session from the broker
                broker.destroySession(this);
                registered = false;
                log.debug("Session({}) disconnected from Broker", cId());
            }
        }
        // send will message
        if (willMessage != null) {
            log.debug("Session({}) closed before Disconnect, now send Will: {}", cId(), willMessage);
            onPublish(willMessage);
            willMessage = null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"registered\":").append(registered).append(',');
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        sb.append("\"cleanSession\":").append(cleanSession()).append(',');
        sb.append("\"bound\":").append(isBound()).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    private Publish extractWillMessage(Connect connect) {
        int qos = connect.willQos();
        String topic = connect.willTopic();
        ByteBuf byteBuf = connect.willMessage();
        boolean retain = connect.willRetainFlag();
        return Publish.outgoing(retain, (byte) qos, false, topic, (short) 0, byteBuf);
    }

}
