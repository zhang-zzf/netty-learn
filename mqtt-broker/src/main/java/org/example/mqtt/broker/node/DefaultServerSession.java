package org.example.mqtt.broker.node;

import static org.example.mqtt.model.ControlPacket.DISCONNECT;
import static org.example.mqtt.model.ControlPacket.PUBLISH;
import static org.example.mqtt.model.ControlPacket.SUBSCRIBE;
import static org.example.mqtt.model.ControlPacket.UNSUBSCRIBE;
import static org.example.mqtt.model.Publish.META_NM_RECEIVE;
import static org.example.mqtt.model.Publish.META_NM_WRAP;
import static org.example.mqtt.model.Publish.META_P_RECEIVE_MILLIS;
import static org.example.mqtt.model.Publish.META_P_RECEIVE_NANO;
import static org.example.mqtt.model.Publish.META_P_SOURCE;
import static org.example.mqtt.model.Publish.META_P_SOURCE_BROKER;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Disconnect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.SubAck;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.UnsubAck;
import org.example.mqtt.model.Unsubscribe;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.session.ControlPacketContext;
import org.example.mqtt.session.Session;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-06
 */
@Slf4j
public class DefaultServerSession extends AbstractSession implements ServerSession {

    private final Broker broker;
    protected Set<Subscribe.Subscription> subscriptions = new HashSet<>();
    private final Queue<ControlPacketContext> inQueue = new LinkedList<>();
    private final Queue<ControlPacketContext> outQueue = new LinkedList<>();

    /**
     * Will Message
     * <pre>
     *     initiate by Connect if will flag is present. It will be cleaned after
     *     1. receive Disconnect
     *     2. lost the Channel with the Client, and forward the message to relative Topic.
     * </pre>
     */
    private volatile Publish willMessage;

    public DefaultServerSession(Connect connect, Channel channel, Broker broker) {
        super(connect.clientIdentifier(), connect.cleanSession(), channel);
        this.broker = broker;
        if (connect.willFlag()) {
            this.willMessage = extractWillMessage(connect);
        }
    }

    @Override
    public void onPacket(ControlPacket packet) {
        if (packet.type() == PUBLISH) {
            if (broker.block(((Publish) packet))) {
                return;
            }
        }
        switch (packet.type()) {
            case SUBSCRIBE -> doReceiveSubscribe((Subscribe) packet);
            case UNSUBSCRIBE -> doReceiveUnsubscribe((Unsubscribe) packet);
            case DISCONNECT -> doReceiveDisconnect((Disconnect) packet);
            default -> super.onPacket(packet);
        }
    }

    @Override
    public Set<Subscribe.Subscription> subscriptions() {
        return subscriptions;
    }

    /**
     * rewrite to handle Publish packet with extra logic
     * <p>ServerSession 需要对发送的 Publish 包 特殊处理</p>
     * <p>ServerSession 只发送 Publish</p>
     */
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
    protected void onPublish(Publish packet) {
        broker.handlePublish(packet);
    }

    @Override
    public Broker broker() {
        return broker;
    }

    @Override
    public void close() {
        super.close();
        // send will message
        if (willMessage != null) {
            log.debug("Session({}) closed before Disconnect, now send Will: {}", cId(), willMessage);
            onPublish(willMessage);
            willMessage = null;
        }
        broker.detachSession(this, false);
    }

    @Override
    public String toString() {
        //   todo
        return "todo";
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
    protected void publishPacketSent(ControlPacketContext cpx) {
        try {
            metricPublish(cpx);
        } catch (Throwable e) {
            // just log an error
            log.error("unExpected exception", e);
        }
        super.publishPacketSent(cpx);
    }

    public static final String METRIC_NAME = DefaultServerSession.class.getName();

    private void metricPublish(ControlPacketContext cpx) {
        Publish packet = cpx.packet();
        Map<String, Object> meta = packet.meta();
        if (meta == null) {
            return;
        }
        long now = System.currentTimeMillis();
        Long pReceive = (Long) meta.get(META_P_RECEIVE_MILLIS);
        if (META_P_SOURCE_BROKER.equals(meta.get(META_P_SOURCE))) {
            // Publish may come from another Broker
            Long nmWrap = (Long) meta.get(META_NM_WRAP);
            Long nmReceive = (Long) meta.get(META_NM_RECEIVE);
            // packetReceive->nmWrap 和 packetReceive->.->packetSent 使用同样的流程，不再重复打点
            // MetricUtil.time(METRIC_NAME, nmWrap - pReceive, "phase", "packetReceive->nmWrap");
            MetricUtil.time(METRIC_NAME, nmReceive - nmWrap, "phase", "nmWrap->nmReceive");
            // nmReceive->packetSent 和 packetReceive->.->packetSent 使用同样的流程，不再重复打点
            // MetricUtil.time(METRIC_NAME, now - nmReceive, "phase", "nmReceive->packetSent");
            MetricUtil.time(METRIC_NAME, now - pReceive, "phase", "packetReceive->nm->packetSent");
        } else {
            long nanoTime = System.nanoTime();
            long pReceiveInNano = (long) meta.get(META_P_RECEIVE_NANO);
            MetricUtil.nanoTime(METRIC_NAME, nanoTime - pReceiveInNano, "phase", "packetReceive->.->packetSent");
        }
        // Public come from Client directly or through another Broker
        // the whole time between Publish.Receive from Client and forward to another Client.
        if (pReceive != null) {
            MetricUtil.time(METRIC_NAME, now - pReceive, "phase", "packetReceive->packetSent");
        }
    }

    @Override
    public DefaultServerSession migrate(Session session) {
        super.migrate(session);
        if (session instanceof DefaultServerSession dss) {
            // subscription
            subscriptions.addAll(dss.subscriptions());
            inQueue.addAll(dss.inQueue);
            outQueue.addAll(dss.outQueue);
        } else {
            throw new IllegalArgumentException();
        }
        return this;
    }

    protected void doReceiveSubscribe(Subscribe packet) {
        log.debug("Session({}) doReceiveSubscribe req: {}", cId(), packet);
        // register the Subscribe packet
        List<Subscribe.Subscription> permitted = broker.subscribe(this, packet);
        this.subscriptions.addAll(permitted);
        SubAck subAck = SubAck.from(packet.packetIdentifier(), permitted);
        log.debug("Session({}) doReceiveSubscribe resp: {}", cId(), subAck);
        doSendPacket(subAck);
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

    protected void doReceiveUnsubscribe(Unsubscribe packet) {
        log.info("Session({}) doReceiveUnsubscribe req: {}", cId(), packet);
        broker.unsubscribe(this, packet);
        packet.subscriptions().forEach(this.subscriptions::remove);
        UnsubAck unsubAck = UnsubAck.from(packet.packetIdentifier());
        log.info("Session({}) doReceiveUnsubscribe resp: {}", cId(), unsubAck);
        doSendPacket(unsubAck);
    }

    private void doReceiveDisconnect(Disconnect packet) {
        log.debug("Session({}) doReceiveDisconnect.", clientIdentifier());
        // clean the Will message.
        if (willMessage != null) {
            log.debug("Session({}) Disconnect, now clear Will: {}", cId(), willMessage);
            willMessage = null;
        }
        close();
    }

    private static Publish extractWillMessage(Connect connect) {
        int qos = connect.willQos();
        String topic = connect.willTopic();
        ByteBuf byteBuf = connect.willMessage();
        boolean retain = connect.willRetainFlag();
        return Publish.outgoing(retain, (byte) qos, false, topic, (short) 0, byteBuf);
    }

}
