package org.example.mqtt.broker.cluster;

import static org.example.mqtt.session.ControlPacketContext.Type.IN;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.node.DefaultServerSession;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Subscribe.Subscription;
import org.example.mqtt.session.ControlPacketContext;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 *
 * <p> Session 的生命周期由 Broker 控制 </p>
 */
@Slf4j
// todo 不继承 DefaultServerSession
public class ClusterServerSession extends DefaultServerSession {

    private volatile String nodeId;
    /**
     * Session 离线时 OutQueue 的 tail
     */
    private volatile Short outQueuePacketIdentifier;

    public ClusterServerSession(String clientIdentifier, Channel channel, Broker broker) {
        super(clientIdentifier, false, channel, broker);
        this.outQueuePacketIdentifier = null;
    }

    public static ClusterServerSession from(String clientIdentifier,
        String nodeId,
        Set<Subscribe.Subscription> subscriptions,
        Short outQueuePacketIdentifier) {
        // todo
        ClusterServerSession s = new ClusterServerSession(clientIdentifier, mockOne(), null);
        s.subscriptions = subscriptions == null ? new HashSet<>(4) : subscriptions;
        s.nodeId = nodeId;
        s.outQueuePacketIdentifier(outQueuePacketIdentifier);
        return s;
    }

    private static Channel mockOne() {
        return new NioSocketChannel();
    }

    @Override
    protected Queue<ControlPacketContext> newInQueue() {
        log.debug("Session({}) now build inQueue", cId());
        return new ClusterDbQueue(clusterDbRepo(), cId(), IN);
    }

    private ClusterDbRepo clusterDbRepo() {
        return broker().clusterDbRepo();
    }

    @Override
    protected Queue<ControlPacketContext> newOutQueue() {
        log.debug("Session({}) now build outQueue.", cId());
        return new ClusterDbQueue(clusterDbRepo(), cId(), OUT);
    }

    @Override
    protected ControlPacketContext findControlPacketInOutQueue(short packetIdentifier) {
        // head was always store in LocalNode
        ControlPacketContext head = outQueue().peek();
        if (head != null && packetIdentifier == head.packetIdentifier()) {
            return head;
        }
        return clusterDbRepo().getCpx(cId(), OUT, packetIdentifier);
    }

    @Override
    protected ControlPacketContext findControlPacketInInQueue(short packetIdentifier) {
        // head was always store in LocalNode
        ControlPacketContext head = inQueue().peek();
        if (head != null && packetIdentifier == head.packetIdentifier()) {
            return head;
        }
        return clusterDbRepo().getCpx(cId(), IN, packetIdentifier);
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
    public void close() {
        super.close();
        if (isOnline()) {
            this.nodeId = null;
            log.debug("Cluster now try to disconnect this Session from Node->{}", this);
            // 保存 tail
            this.outQueuePacketIdentifier = ((ClusterDbQueue) outQueue()).tailPacketIdentifier();
            broker().disconnectSessionFromNode(this);
            log.debug("the cleanSession=0 Session was disconnected from this Node");
        }
    }

    @Override
    public String toString() {
        // todo
        return "todo";
    }

    @Override
    public ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }

    public Short outQueuePacketIdentifier() {
        return outQueuePacketIdentifier;
    }

    // public void outQueuePacketIdentifier(Short packetIdentifier) {
    //     this.outQueuePacketIdentifier = packetIdentifier;
    //     this.resetPacketIdentifier(packetIdentifier);
    // }
    //
    // public void resetPacketIdentifier(Short packetIdentifier) {
    //     if (packetIdentifier == null) {
    //         // do nothing
    //         return;
    //     }
    //     if (packetIdentifier >= Short.MAX_VALUE) {
    //         throw new IllegalArgumentException();
    //     }
    //     this.packetIdentifier.set(packetIdentifier);
    // }

    public boolean isOnline() {
        return nodeId != null;
    }

    // public ClusterServerSession nodeId(String nodeId) {
    //     this.nodeId = nodeId;
    //     return this;
    // }

}