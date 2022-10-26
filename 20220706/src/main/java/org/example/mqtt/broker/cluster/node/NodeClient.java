package org.example.mqtt.broker.cluster.node;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.EventLoopGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.client.Client;
import org.example.mqtt.client.MessageHandler;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.example.mqtt.broker.cluster.node.Cluster.$_SYS_NODE_TOPIC;
import static org.example.mqtt.broker.cluster.node.Cluster.subscribeAllNode;
import static org.example.mqtt.broker.cluster.node.NodeMessage.*;

@Slf4j
public class NodeClient implements MessageHandler, AutoCloseable {

    private final Client client;
    private final Cluster cluster;
    private final Node remoteNode;
    @Getter
    private final String clientIdentifier;
    private final static AtomicLong clientIdentifierCounter = new AtomicLong(0);

    public NodeClient(Node remoteNode, EventLoopGroup clientEventLoopGroup, Cluster cluster) {
        this.clientIdentifier = String.format($_SYS_NODE_TOPIC,
                cluster.broker().nodeId(), clientIdentifierCounter.getAndIncrement());
        this.remoteNode = remoteNode;
        this.cluster = cluster;
        this.client = new Client(clientIdentifier, remoteNode.address(), clientEventLoopGroup, this);
        subscribeMyClientIdentifier();
    }

    public void subscribeClusterMessage() {
        Subscribe.Subscription clusterNodes =
                new Subscribe.Subscription(subscribeAllNode(), Publish.AT_LEAST_ONCE);
        List<Subscribe.Subscription> sub = Arrays.asList(clusterNodes);
        log.info("NodeClient try to subscribe-> client: {}, Topic: {}", clientIdentifier, sub);
        client.syncSubscribe(sub);
    }

    private void subscribeMyClientIdentifier() {
        Subscribe.Subscription nodeSubscription =
                new Subscribe.Subscription(clientIdentifier, Publish.EXACTLY_ONCE);
        List<Subscribe.Subscription> sub = Arrays.asList(nodeSubscription);
        log.debug("NodeClient try to subscribe-> client: {}, Topic: {}", clientIdentifier, sub);
        client.syncSubscribe(sub);
    }

    private ClusterBroker broker() {
        return cluster.broker();
    }

    @Override
    public void handle(String topic, Publish packet) {
        ByteBuf payload = packet.payload();
        NodeMessage m;
        if ((byte) '{' == payload.getByte(0)) {
            // json NodeMessage
            m = NodeMessage.fromBytes(ByteBufUtil.getBytes(payload));
        } else {
            // binary protocol Publish NodeMessage
            m = new NodePublish(payload);
        }
        log.debug("NodeClient receive message-> ncId:{}, msg: {}", m);
        doHandleNodeMessageWithMetric(m);
    }

    private void doHandleNodeMessageWithMetric(NodeMessage m) {
        long start = System.currentTimeMillis();
        try {
            doHandleNodeMessage(m);
        } finally {
            long time = System.currentTimeMillis() - start;
            MetricUtil.time("cluster.node.NodeMessage", time,
                    "packet", m.getPacket(),
                    "from", m.getNodeId(),
                    "target", broker().nodeId()
            );
        }
    }

    private void doHandleNodeMessage(NodeMessage m) {
        switch (m.getPacket()) {
            case ACTION_PUBLISH_FORWARD:
                doHandleActionPublishForward((NodePublish) m);
                break;
            case INFO_CLUSTER_NODES:
                doHandleInfoClusterNodes(m);
                break;
            case ACTION_SESSION_CLOSE:
                doHandleActionSessionClose(m);
                break;
            case INFO_CLIENT_CONNECT:
                doHandleClientConnect(m);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void doHandleActionSessionClose(NodeMessage m) {
        String clientIdentifier = m.unwrapSessionClose();
        ServerSession session = broker().session(clientIdentifier);
        log.info("NodeClient receive Session.Closed. cur Session->{}", session);
        if (session != null) {
            session.close();
            log.info("NodeClient Session.Closed->{}", clientIdentifier);
        } else {
            log.warn("NodeClient does not exist Session({})", clientIdentifier);
        }
    }

    private void doHandleInfoClusterNodes(NodeMessage m) {
        Set<NodeInfo> state = m.unwrapClusterNodes();
        log.debug("NodeClient receive Cluster.Nodes-> ncId: {}, remote: {}, remoteState: {}",
                clientIdentifier, m.getNodeId(), state);
        cluster.updateNodes(m.getNodeId(), state);
    }

    private void doHandleActionPublishForward(NodePublish m) {
        // forward Publish
        Publish packet = m.unwrapPublish();
        log.debug("NodeClient receive Publish: {}", packet);
        // just use nodeBroker to forward the PublishPacket
        broker().nodeBroker().forward(packet);
    }

    private void doHandleClientConnect(NodeMessage m) {
        String clientIdentifier = m.unwrapSessionClose();
        // just get Session from local Node(Broker)
        ServerSession session = broker().nodeBroker().session(clientIdentifier);
        // session is null in normal case
        if (session != null) {
            if (session instanceof ClusterServerSession) {
                // should not exist
                log.error("Client.Connect from {}, Node has a ClusterServerSession", m.getNodeId(), session);
                return;
            }
            log.info("Client.Connect from {}, now close Session: {}", m.getNodeId(), session);
            session.close();
            broker().destroySession(session);
        }
    }

    @Override
    public void clientClosed() {
        log.info("NodeClient({}) clientClosed", this);
        boolean removed = remoteNode.removeNodeClient(this);
        if (!removed) {
            log.error("NodeClient remove from Node failed-> client:{}, Node: {}", this, remoteNode);
            return;
        }
        log.info("NodeClient removed from Node-> client:{}, Node: {}", this, remoteNode);
        if (remoteNode.nodeClientsCnt() == 0) {
            cluster.removeNode(remoteNode);
        }
    }

    @Override
    public void close() throws Exception {
        log.info("NodeClient({}) now try to close", this);
        client.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (clientIdentifier != null) {
            sb.append("\"clientIdentifier\":\"").append(clientIdentifier).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public void syncSend(int qos, String topic, ByteBuf payload) throws ExecutionException, InterruptedException {
        client.syncSend(qos, topic, payload);
    }

}
