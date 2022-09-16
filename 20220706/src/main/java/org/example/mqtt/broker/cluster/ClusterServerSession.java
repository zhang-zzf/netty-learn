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

import static org.example.mqtt.broker.cluster.ClusterControlPacketContext.packetIdentifier;

@Slf4j
public class ClusterServerSession extends DefaultServerSession {

    private final ClusterDbRepo clusterDbRepo;
    private String nodeId;
    /**
     * Session 离线时 OutQueue 的 tail
     */
    // todo -> packetIdentifier
    private String outQueueTailWhenOffline;

    public ClusterServerSession(String clientIdentifier, ClusterDbRepo clusterDbRepo) {
        super(clientIdentifier);
        this.clusterDbRepo = clusterDbRepo;
    }

    public static ClusterServerSession from(ClusterDbRepo clusterDbRepo,
                                            String clientIdentifier, String nodeId,
                                            Set<Subscribe.Subscription> subscriptions,
                                            String outQueueTail) {
        ClusterServerSession s = new ClusterServerSession(clientIdentifier, clusterDbRepo);
        s.subscriptions = subscriptions == null ? new HashSet<>(4) : subscriptions;
        s.nodeId = nodeId;
        s.outQueueTailWhenOffline(outQueueTail);
        return s;
    }

    public static ClusterServerSession from(ClusterDbRepo clusterDbRepo, Connect connect) {
        ClusterServerSession s = new ClusterServerSession(connect.clientIdentifier(), clusterDbRepo);
        s.reInitWith(connect);
        return s;
    }

    @Override
    protected Queue<ControlPacketContext> newInQueue() {
        return new ClusterDbQueue(clusterDbRepo, cId(), ClusterDbQueue.Type.IN_QUEUE);
    }

    @Override
    protected Queue<ControlPacketContext> newOutQueue() {
        return new ClusterDbQueue(clusterDbRepo, cId(), ClusterDbQueue.Type.OUT_QUEUE);
    }

    @Override
    protected ControlPacketContext findControlPacketInOutQueue(short packetIdentifier) {
        return clusterDbRepo.getCpxFromSessionQueue(cId(), ClusterDbQueue.Type.OUT_QUEUE, packetIdentifier);
    }

    @Override
    protected ControlPacketContext findControlPacketInInQueue(short packetIdentifier) {
        return clusterDbRepo.getCpxFromSessionQueue(cId(), ClusterDbQueue.Type.IN_QUEUE, packetIdentifier);
    }

    @Override
    protected ControlPacketContext createNewCpx(Publish packet,
                                                ControlPacketContext.Status status,
                                                ControlPacketContext.Type type) {
        return new ClusterControlPacketContext(clusterDbRepo, cId(), type, packet, status, null);
    }

    @Override
    protected void doAddSubscriptions(List<Subscribe.Subscription> permitted) {
        // update the Node Session
        super.doAddSubscriptions(permitted);
        // save the Cluster Session
        clusterDbRepo.saveSession(this);
    }

    @Override
    protected void doRemoveSubscriptions(List<Subscribe.Subscription> subscriptions) {
        // update the Node Session
        super.doRemoveSubscriptions(subscriptions);
        // save the Cluster Session
        clusterDbRepo.saveSession(this);
    }

    public String nodeId() {
        return nodeId;
    }

    @Override
    public void open(Channel ch, Broker broker) {
        if (this.nodeId != null) {
            log.warn("nodeId is not null");
        }
        this.nodeId = ((ClusterBroker) broker).nodeId();
        this.outQueueTailWhenOffline = null;
        // 注册成功,绑定信息保存到 DB
        clusterDbRepo.saveSession(this);
        super.open(ch, broker);
    }

    /**
     * @param force force clean Session from Cluster if true
     */
    @Override
    public void close(boolean force) {
        if (!isRegistered()) {
            log.warn("ClusterServerSession is not bound to the Node(Broker)");
            return;
        }
        // 清除本 broker 中的 Session (even if CleanSession=0)
        super.close(true);
        closeClusterSession(force);
    }

    public void closeClusterSession(boolean force) {
        if (force) {
            // 清除 cluster leven Session
            clusterDbRepo.deleteSession(this);
        } else {
            this.nodeId = null;
            // 保存 tail
            List<ClusterControlPacketContext> tail = clusterDbRepo.searchSessionQueue(
                    clientIdentifier(), ClusterDbQueue.Type.OUT_QUEUE, true, 1);
            this.outQueueTailWhenOffline = tail.isEmpty() ? null : tail.get(0).id();
            clusterDbRepo.saveSession(this);
        }
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"registered\":").append(isRegistered()).append(',');
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        sb.append("\"cleanSession\":").append(cleanSession()).append(',');
        if (nodeId() != null) {
            sb.append("\"nodeId\":\"").append(nodeId()).append("\",");
        }
        if (outQueueTailWhenOffline() != null) {
            sb.append("\"outQueueTailWhenOffline\":\"").append(outQueueTailWhenOffline()).append("\",");
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    @Override
    public boolean isBound() {
        return nodeId() != null;
    }

    @Override
    public ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }


    public String outQueueTailWhenOffline() {
        return outQueueTailWhenOffline;
    }

    public void outQueueTailWhenOffline(String tail) {
        this.outQueueTailWhenOffline = tail;
        this.resetPacketIdentifier(packetIdentifier(tail));
    }

    public void resetPacketIdentifier(Integer packetIdentifier) {
        if (packetIdentifier == null) {
            // do nothing
            return;
        }
        if (packetIdentifier > Short.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        this.packetIdentifier.set(packetIdentifier);
    }

}