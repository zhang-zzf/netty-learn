package org.example.mqtt.broker.cluster;

import static org.example.mqtt.session.ControlPacketContext.Type.IN;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

import io.netty.channel.Channel;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.node.DefaultServerSession;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;
import org.example.mqtt.session.ControlPacketContext;
import org.example.mqtt.session.Session;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-06
 */
@Slf4j
public class ClusterServerSessionImpl extends DefaultServerSession implements ClusterServerSession {

    private final Queue<ControlPacketContext> inQueue;
    private final Queue<ControlPacketContext> outQueue;

    private volatile String nodeId;

    /**
     * 创建之前保证：集群中的同一个 client 不存在 ClusterServerSession
     */
    public ClusterServerSessionImpl(Connect connect, Channel channel, Broker broker) {
        super(connect, channel, broker);
        // ClusterServerSessionImpl only used for a not clean session
        assert !connect.cleanSession();
        // check ClusterServerSession's owner before bind to this broker
        ServerSession session = broker().session(connect.clientIdentifier());
        if (session == null || session instanceof ClusterServerSession css && css.nodeId() == null) {
        }
        else {
            throw new IllegalStateException();
        }
        this.nodeId = broker().nodeId();
        this.inQueue = new ClusterQueue(broker().state(), cId(), IN);
        this.outQueue = new ClusterQueue(broker().state(), cId(), OUT);
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
    public ClusterServerSessionImpl migrate(Session session) {
        if (session instanceof ClusterServerSessionRemoteNodeImpl cssrn) {
            // 集群游离的 session
            // 手动初始化
            super.packetIdentifier.set(cssrn.nextPacketIdentifier());
            super.subscriptions.addAll(cssrn.subscriptions());
        }
        else {
            throw new IllegalArgumentException();
        }
        // watch out: do not use parent migrate
        // super.migrate(session);
        return this;
    }

    @Override
    public ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public boolean isOnline() {
        return nodeId != null;
    }

    @Override
    protected ControlPacketContext findControlPacketInOutQueue(short packetIdentifier) {
        // head was always store in LocalNode
        ControlPacketContext head = outQueue().peek();
        if (head != null && packetIdentifier == head.packetIdentifier()) {
            return head;
        }
        return broker().state().getCpx(cId(), OUT, packetIdentifier);
    }

    @Override
    protected ControlPacketContext findControlPacketInInQueue(short packetIdentifier) {
        // head was always store in LocalNode
        ControlPacketContext head = inQueue().peek();
        if (head != null && packetIdentifier == head.packetIdentifier()) {
            return head;
        }
        return broker().state().getCpx(cId(), IN, packetIdentifier);
    }

    @Override
    protected ControlPacketContext createNewCpx(Publish packet,
        ControlPacketContext.Status status,
        ControlPacketContext.Type type) {
        if (type == OUT && !enqueueOutQueue(packet)) {
            return new ControlPacketContext(packet, status, type);
        }
        if (type == IN && !enqueueInQueue(packet)) {
            return new ControlPacketContext(packet, status, type);
        }
        return new ClusterControlPacketContext(broker().state(),
            cId(), type, packet, status, null);
    }

    @Override
    protected void doReceiveSubscribe(Subscribe packet) {
        super.doReceiveSubscribe(packet);
        // save the Cluster Session
        broker().state().saveSession(this);
    }

    @Override
    protected void doReceiveUnsubscribe(Unsubscribe packet) {
        super.doReceiveUnsubscribe(packet);
        // save the Cluster Session
        broker().state().saveSession(this);
    }

    @Override
    public void close() {
        log.debug("Cluster now try to disconnect this Session from Node -> {}", this);
        super.close();
        this.nodeId = null;
        broker().detachSession(this, false);
        log.debug("Cluster disconnected this Session from Node -> {}", this);
    }

    @Override
    public String toString() {
        // todo
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"type\":\"ClusterServerSession\",");
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        sb.append("\"cleanSession\":").append(cleanSession()).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}