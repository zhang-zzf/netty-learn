package org.github.zzf.mqtt.server;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.DISCONNECT;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PINGREQ;
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
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.Disconnect;
import org.github.zzf.mqtt.protocol.model.PingReq;
import org.github.zzf.mqtt.protocol.model.PingResp;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.SubAck;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.UnsubAck;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.ServerSession;
import org.github.zzf.mqtt.protocol.session.AbstractSession;
import org.github.zzf.mqtt.protocol.session.ControlPacketContext;
import org.github.zzf.mqtt.server.metric.MetricUtil;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-06
 */
@Slf4j
public class DefaultServerSession extends AbstractSession implements ServerSession {

    private final Broker broker;
    private final Set<Subscribe.Subscription> subscriptions = new HashSet<>();
    // todo 监控内存占用
    private final Queue<ControlPacketContext> inQueue = new LinkedList<>();
    private final Queue<ControlPacketContext> outQueue = new LinkedList<>();

    private final Connect connect;
    private final boolean isResumed;

    /**
     * todo use this.connect
     * Will Message
     * <pre>
     *     initiate by Connect if will flag is present. It will be cleaned after
     *     1. receive Disconnect
     *     2. lost the Channel to Client, and forward the message to relative Topic.
     * </pre>
     */
    private volatile Publish willMessage;

    public DefaultServerSession(Connect connect,
            Channel channel,
            DefaultBroker broker,
            ServerSession previous) {
        this(connect, channel, broker, true);
        // The Client and Server MUST store the Session after the Client and Server are disconnected
        // 按照 mqtt 协议 Client 会保存和重新发送未确认的消息。若 Client 未按照协议设计可能导致 inQueue 异常
        // todo inQueue 存在 QoS2 消息，若 Client 重连后没有恢复 QoS2 消息的状态，inQueue 中的消息和后续消息接无法清理
        // todo Client 重连后，如何重发 QoS1 / QoS2 消息 ？
        // 1. QoS1 重新发送消息； QoS2 按状态恢复发送
        // 1. QoS1 重新发送消息； QoS2 重新发送
        // mqtt 协议规定:
        // When a Client reconnects with CleanSession set to 0, both the Client and Server
        // MUST re-send any unacknowledged PUBLISH Packets (where QoS > 0) and
        // PUBREL Packets using their original Packet Identifiers.
        // 通用理解为：  QoS1 重新发送消息； QoS2 按状态恢复发送
        // sender 未收到 PUBREC ->  客户端必须重新发送相同的 PUBLISH 数据包（相同的 Packet ID=100） DUP 标志设置为 1
        // sender 收到 PUBREC ->  客户端必须重新发送 PUBREL 数据包（相同的 Packet ID=100）
        if (previous instanceof DefaultServerSession dss) {
            packetIdentifier.set(dss.packetIdentifier.get());
            // subscription
            subscriptions.addAll(dss.subscriptions());
            inQueue.addAll(dss.inQueue);
            outQueue.addAll(dss.outQueue);
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    public DefaultServerSession(Connect connect,
            Channel channel,
            Broker broker) {
        this(connect, channel, broker, false);
    }

    public DefaultServerSession(Connect connect,
            Channel channel,
            Broker broker,
            boolean isResumed) {
        super(connect.clientIdentifier(), connect.cleanSession(), channel);
        this.broker = broker;
        this.connect = connect;
        this.isResumed = isResumed;
        if (connect.willFlag()) {
            this.willMessage = extractWillMessage(connect);
        }

    }

    @Override
    public ChannelFuture send(ControlPacket packet) {
        if (packet instanceof Publish publish) {
            publish.payload().retain();
            log.debug("sender({}/{}) Publish . -> [RETAIN] payload.refCnt: {}", cId(), publish.pId(), publish.payload().refCnt());
        }
        return super.send(packet);
    }

    @Override
    public void onPacket(ControlPacket packet) {
        switch (packet.type()) {
            case PINGREQ -> doReceivePingReq((PingReq) packet);
            case SUBSCRIBE -> doReceiveSubscribe((Subscribe) packet);
            case UNSUBSCRIBE -> doReceiveUnsubscribe((Unsubscribe) packet);
            case DISCONNECT -> doReceiveDisconnect((Disconnect) packet);
            default -> super.onPacket(packet);
        }
    }

    private void doReceivePingReq(PingReq packet) {
        send(new PingResp());
    }

    @Override
    public Set<Subscribe.Subscription> subscriptions() {
        return subscriptions;
    }

    /**
     * <p>ServerSession 只发送 Publish</p>
     * <p> todo ServerSession 发送 SubAck 等</p>
     */
    // @Override
    // public ChannelFuture send(ControlPacket packet) {
    //     if (packet == null || packet.type() != PUBLISH) {
    //         throw new IllegalArgumentException();
    //     }
    //     return super.send(packet);
    // }
    @Override
    protected void onPublish(Publish packet) {
        broker.forward(packet);
    }

    @Override
    public Broker broker() {
        return broker;
    }

    // todo
    // @Override
    // public void close() {
    //     if (closing) {
    //         return;
    //     }
    //     this.closing = true;
    //     // send will message
    //     if (willMessage != null) {
    //         log.debug("Session({}) closed before Disconnect, now send Will: {}", cId(), willMessage);
    //         onPublish(willMessage);
    //         willMessage = null;
    //     }
    //     log.debug("Session({}) now try to closed -> {}", cId(), this);
    //     broker.detachSession(this, false);
    //     super.close();
    // }
    //
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"session\":\"").append(this.getClass().getSimpleName()).append('\"').append(',');
        sb.append("\"super\":").append(super.toString()).append(',');
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
    protected void publishSent(Publish packet) {
        try {
            metricPublish(packet);
        } catch (Throwable e) {
            // just log an error
            log.error("unExpected exception", e);
        }
        super.publishSent(packet);
    }

    public static final String METRIC_NAME = DefaultServerSession.class.getName();

    private void metricPublish(Publish packet) {
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
        }
        else {
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

    protected void doReceiveSubscribe(Subscribe packet) {
        log.debug("Session({}) doReceiveSubscribe req: {}", cId(), packet);
        // register the Subscribe packet
        List<Subscribe.Subscription> permitted = broker.subscribe(this, packet.subscriptions());
        this.subscriptions.addAll(permitted);
        SubAck subAck = SubAck.from(packet.packetIdentifier(), permitted);
        log.debug("Session({}) doReceiveSubscribe resp: {}", cId(), subAck);
        send(subAck);
    }

    protected void doReceiveUnsubscribe(Unsubscribe packet) {
        log.info("Session({}) doReceiveUnsubscribe req: {}", cId(), packet);
        broker.unsubscribe(this, packet.subscriptions());
        packet.subscriptions().forEach(this.subscriptions::remove);
        UnsubAck unsubAck = new UnsubAck(packet.packetIdentifier());
        log.info("Session({}) doReceiveUnsubscribe resp: {}", cId(), unsubAck);
        send(unsubAck);
    }

    private void doReceiveDisconnect(Disconnect packet) {
        log.debug("Session({}) doReceiveDisconnect.", clientIdentifier());
        // clean the Will message.
        if (willMessage != null) {
            log.debug("Session({}) Disconnect, now clear Will: {}", cId(), willMessage);
            willMessage = null;
        }
        // todo
        // close();
    }

    private static Publish extractWillMessage(Connect connect) {
        int qos = connect.willQos();
        String topic = connect.willTopic();
        ByteBuf byteBuf = connect.willMessage();
        boolean retain = connect.willRetainFlag();
        return Publish.outgoing(retain, (byte) qos, false, topic, (short) 0, byteBuf);
    }

    // todo cleanSession 复制 UT
    @Override
    public boolean isResumed() {
        return isResumed;
    }

}
