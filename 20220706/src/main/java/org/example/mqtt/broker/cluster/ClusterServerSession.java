package org.example.mqtt.broker.cluster;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.node.DefaultServerSession;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.session.ControlPacketContext;

import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static org.example.mqtt.session.ControlPacketContext.Type.IN;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

/**
 * Session 的生命周期由 Broker 控制
 */
@Slf4j
public class ClusterServerSession extends DefaultServerSession {

    private volatile String nodeId;
    /**
     * Session 离线时 OutQueue 的 tail
     */
    private Short outQueuePacketIdentifier;

    public ClusterServerSession(String clientIdentifier) {
        super(clientIdentifier);
        super.cleanSession(false);
    }

    public static ClusterServerSession from(String clientIdentifier, String nodeId,
                                            Set<Subscribe.Subscription> subscriptions,
                                            Short outQueuePacketIdentifier) {
        ClusterServerSession s = new ClusterServerSession(clientIdentifier);
        s.subscriptions = subscriptions == null ? new HashSet<>(4) : subscriptions;
        s.nodeId = nodeId;
        s.outQueuePacketIdentifier(outQueuePacketIdentifier);
        return s;
    }

    public static ClusterServerSession from(Connect connect) {
        ClusterServerSession s = new ClusterServerSession(connect.clientIdentifier());
        s.reInitWith(connect);
        return s;
    }

    @Override
    protected Queue<ControlPacketContext> newInQueue() {
        log.debug("Session({}) now build inQueue", cId());
        return new ClusterDbQueue(clusterDbRepo(), cId(), ClusterDbQueue.Type.IN_QUEUE);
    }

    private ClusterDbRepo clusterDbRepo() {
        return broker().clusterDbRepo();
    }

    @Override
    protected Queue<ControlPacketContext> newOutQueue() {
        log.debug("Session({}) now build outQueue.", cId());
        return new ClusterDbQueue(clusterDbRepo(), cId(), ClusterDbQueue.Type.OUT_QUEUE);
    }

    @Override
    protected ControlPacketContext findControlPacketInOutQueue(short packetIdentifier) {
        // head was always store in LocalNode
        ControlPacketContext head = outQueue().peek();
        if (head != null && packetIdentifier == head.packetIdentifier()) {
            return head;
        }
        return clusterDbRepo().getCpxFromSessionQueue(cId(), ClusterDbQueue.Type.OUT_QUEUE, packetIdentifier);
    }

    @Override
    protected ControlPacketContext findControlPacketInInQueue(short packetIdentifier) {
        // head was always store in LocalNode
        ControlPacketContext head = inQueue().peek();
        if (head != null && packetIdentifier == head.packetIdentifier()) {
            return head;
        }
        return clusterDbRepo().getCpxFromSessionQueue(cId(), ClusterDbQueue.Type.IN_QUEUE, packetIdentifier);
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
        return new ClusterControlPacketContext(clusterDbRepo(),
                cId(), type, packet, status, null);
    }

    @Override
    protected void doAddSubscriptions(List<Subscribe.Subscription> permitted) {
        // update the Node Session
        super.doAddSubscriptions(permitted);
        // save the Cluster Session
        clusterDbRepo().saveSession(this);
    }

    @Override
    protected void doRemoveSubscriptions(List<Subscribe.Subscription> subscriptions) {
        // update the Node Session
        super.doRemoveSubscriptions(subscriptions);
        // save the Cluster Session
        clusterDbRepo().saveSession(this);
    }

    public String nodeId() {
        return nodeId;
    }

    @Override
    public void open(Channel ch, Broker broker) {
        log.debug("Cluster Session({}) Channel<->Session<->Broker", cId());
        super.open(ch, broker);
        if (this.nodeId != null) {
            log.debug("Session({}) nodeId is Not Null: {}", cId(), nodeId());
        }
        ClusterBroker cb = (ClusterBroker) broker;
        this.nodeId = cb.nodeId();
        this.outQueuePacketIdentifier = null;
        // 注册成功,绑定信息保存到 DB
        cb.clusterDbRepo().saveSession(this);
    }

    @Override
    public void close() {
        log.debug("Cluster now try close Session->{}", this);
        if (!isOnline()) {
            log.warn("ClusterServerSession is not bound to the Node(Broker)");
            return;
        }
        disconnectSessionFromThisNode();
        super.close();
    }

    private void disconnectSessionFromThisNode() {
        if (isOnline()) {
            this.nodeId = null;
            // 保存 tail
            List<ClusterControlPacketContext> tail =
                    clusterDbRepo().searchSessionQueue(clientIdentifier(), ClusterDbQueue.Type.OUT_QUEUE, true, 1);
            this.outQueuePacketIdentifier = tail.isEmpty() ? null : tail.get(0).packetIdentifier();
            // 清除本 broker 中的 Session (even if CleanSession=0)
            log.debug("Cluster now try to disconnect the cleanSession=0 Session from this Node->{}", this);
            clusterDbRepo().saveSession(this);
            log.debug("the cleanSession=0 Session was disconnected from this Node");
            broker().nodeBroker().destroySession(this);
        }
    }

    @Override
    public void channelClosed() {
        super.channelClosed();
        disconnectSessionFromThisNode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"type\":\"ClusterServerSession\",");
        sb.append("\"registered\":").append(isRegistered()).append(',');
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        sb.append("\"cleanSession\":").append(cleanSession()).append(',');
        sb.append("\"bound\":").append(isBound()).append(',');
        sb.append("\"isOnline\":").append(isOnline()).append(',');
        if (nodeId() != null) {
            sb.append("\"nodeId\":\"").append(nodeId()).append("\",");
        }
        if (outQueuePacketIdentifier() != null) {
            sb.append("\"outQueuePacketIdentifier\":").append(outQueuePacketIdentifier()).append(",");
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    @Override
    public ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }


    public Short outQueuePacketIdentifier() {
        return outQueuePacketIdentifier;
    }

    public void outQueuePacketIdentifier(Short packetIdentifier) {
        this.outQueuePacketIdentifier = packetIdentifier;
        this.resetPacketIdentifier(packetIdentifier);
    }

    public void resetPacketIdentifier(Short packetIdentifier) {
        if (packetIdentifier == null) {
            // do nothing
            return;
        }
        if (packetIdentifier >= Short.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        this.packetIdentifier.set(packetIdentifier);
    }

    public boolean isOnline() {
        return nodeId() != null;
    }

}