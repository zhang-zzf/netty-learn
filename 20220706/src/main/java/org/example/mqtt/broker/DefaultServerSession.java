package org.example.mqtt.broker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.example.mqtt.model.ControlPacket.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public class DefaultServerSession extends AbstractSession implements ServerSession {

    private Broker broker;
    private volatile boolean registered;
    private Set<Subscribe.Subscription> subscriptions = new HashSet<>();
    /**
     * Disconnect will change it to true.
     */
    private boolean disconnected = false;
    /**
     * Will Message
     * <pre>
     *     initiate by Connect if will flag is present. It will be cleaned after
     *     1. receive Disconnect
     *     2. lost the Channel with the Client, and forward the message to relative Topic.
     * </pre>
     */
    private Publish willMessage;

    public DefaultServerSession(Connect connect) {
        super(connect.clientIdentifier());
        cleanSession(connect.cleanSession());
        if (connect.willFlag()) {
            willMessage = extractWillMessage(connect);
        }
    }

    @Override
    public void messageReceived(ControlPacket packet) {
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
    public Set<Subscribe.Subscription> subscriptions() {
        return subscriptions;
    }

    @Override
    public Future<Void> send(ControlPacket packet) {
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        if (packet.type() == PUBLISH) {
            Publish publish = (Publish) packet;
            /*
              {@link DefaultServerSession#publishSent(ControlPacketContext)}  will release the payload
             */
            publish.payload().retain();
            return sendPublishInEventLoop(publish);
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
        broker.forward(packet);
        return true;
    }

    private void doReceiveUnsubscribe(Unsubscribe packet) {
        log.info("doReceiveUnsubscribe req: {}, {}", clientIdentifier(), packet);
        broker().deregister(this, packet);
        UnsubAck unsubAck = UnsubAck.from(packet.packetIdentifier());
        log.info("client({}) doReceiveUnsubscribe resp: {}", clientIdentifier(), unsubAck);
        channel().writeAndFlush(unsubAck);
    }

    private void doReceiveSubscribe(Subscribe packet) {
        log.info("client({}) doReceiveSubscribe req: {}", clientIdentifier(), packet);
        // register the Subscribe
        List<Subscribe.Subscription> permitted = broker().subscribe(this, packet);
        SubAck subAck = SubAck.from(packet.packetIdentifier(), permitted);
        log.info("client({}) doReceiveSubscribe resp: {}", clientIdentifier(), subAck);
        channel().writeAndFlush(subAck);
        doUpdateSessionSubscriptions(permitted);
    }

    private void doUpdateSessionSubscriptions(List<Subscribe.Subscription> permitted) {
        for (Subscribe.Subscription p : permitted) {
            boolean exists = false;
            // todo 参考 mqtt 协议：待优化
            for (Subscribe.Subscription sub : subscriptions) {
                if (sub.topicFilter().equals(p.topicFilter())) {
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
        log.info("Session({}) doReceiveDisconnect.", clientIdentifier());
        disconnected = true;
        // clean the Will message.
        willMessage = null;
        // 断开与 Broker 的连接。移除 Broker 中和 Session 有关的状态
        if (cleanSession()) {
            // 取消与 Broker 的关联
            deregister();
        }
        // 关闭底层用于通信的 Channel
        closeChannel();
    }

    @Override
    public Broker broker() {
        return broker;
    }

    @Override
    public void register(Broker broker) {
        if (!registered) {
            this.broker = broker;
            broker().connect(this);
            registered = true;
        }
    }

    @Override
    public boolean isRegistered() {
        return registered;
    }

    @Override
    public void open(Channel ch, Broker broker) {
        bind(ch);
        register(broker);
    }

    @Override
    public void close() {
        // send will message
        if (willMessage != null) {
            broker.forward(willMessage);
            willMessage = null;
        }
        // 取消与 Broker 的关联
        deregister();
        // 关闭底层连接
        closeChannel();
    }

    @Override
    public void deregister() {
        if (registered) {
            // disconnect the session from the broker
            broker().disconnect(this);
            registered = false;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"registered\":").append(registered).append(',');
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        sb.append("\"cleanSession\":").append(cleanSession()).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    private Publish extractWillMessage(Connect connect) {
        int qos = connect.willQos();
        String topic = connect.willTopic();
        ByteBuf byteBuf = connect.willMessage();
        return Publish.outgoing(false, (byte) qos, false, topic, (short) 0, byteBuf);
    }

}
