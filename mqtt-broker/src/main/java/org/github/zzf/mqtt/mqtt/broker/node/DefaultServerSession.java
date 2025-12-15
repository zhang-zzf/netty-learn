package org.github.zzf.mqtt.mqtt.broker.node;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.DISCONNECT;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PUBLISH;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.SUBSCRIBE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.UNSUBSCRIBE;
import static org.github.zzf.mqtt.protocol.model.Publish.META_NM_RECEIVE;
import static org.github.zzf.mqtt.protocol.model.Publish.META_NM_WRAP;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_RECEIVE_MILLIS;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_RECEIVE_NANO;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_SOURCE;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_SOURCE_BROKER;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import io.netty.channel.ChannelFuture;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.micrometer.utils.MetricUtil;
import org.github.zzf.mqtt.mqtt.broker.Broker;
import org.github.zzf.mqtt.mqtt.broker.ServerSession;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.Disconnect;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.SubAck;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.UnsubAck;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;
import org.github.zzf.mqtt.mqtt.session.AbstractSession;
import org.github.zzf.mqtt.mqtt.session.ControlPacketContext;
import org.github.zzf.mqtt.mqtt.session.Session;

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
     * <p>ServerSession 只发送 Publish</p>
     */
    @Override
    public ChannelFuture send(ControlPacket packet) {
        if (packet == null || packet.type() != PUBLISH) {
            throw new IllegalArgumentException();
        }
        return super.send(packet);
    }

    @Override
    protected void onPublish(Publish packet) {
        broker.handlePublish(packet);
    }

    @Override
    public Broker broker() {
        return broker;
    }

    private volatile boolean closing = false;

    @Override
    public void close() {
        if (closing) {
            return;
        }
        this.closing = true;
        // send will message
        if (willMessage != null) {
            log.debug("Session({}) closed before Disconnect, now send Will: {}", cId(), willMessage);
            onPublish(willMessage);
            willMessage = null;
        }
        log.debug("Session({}) now try to closed -> {}", cId(), this);
        broker.detachSession(this, false);
        super.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"session\":\"").append(this.getClass().getSimpleName()).append('\"').append(',');
        sb.append("\"super\":").append(super.toString()).append(',');
        if (subscriptions != null) {
            sb.append("\"subscriptions\":");
            if (!(subscriptions).isEmpty()) {
                sb.append("[");
                for (Object collectionValue : subscriptions) {
                    sb.append("\"").append(Objects.toString(collectionValue, "")).append("\",");
                }
                sb.replace(sb.length() - 1, sb.length(), "]");
            }
            else {
                sb.append("[]");
            }
            sb.append(',');
        }
        sb.append("\"inQueue\":").append(inQueue.size()).append(",");
        sb.append("\"outQueue\":").append(outQueue.size()).append(",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
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
                // todo retailFlag will be false or true
                // do rebuild the PublishPacket
                send(Publish.outgoing(packet, p.topicFilter(), (byte) qos, nextPacketIdentifier()));
            }
        }
    }

    protected void doReceiveUnsubscribe(Unsubscribe packet) {
        log.info("Session({}) doReceiveUnsubscribe req: {}", cId(), packet);
        broker.unsubscribe(this, packet);
        packet.subscriptions().forEach(this.subscriptions::remove);
        UnsubAck unsubAck = new UnsubAck(packet.packetIdentifier());
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
