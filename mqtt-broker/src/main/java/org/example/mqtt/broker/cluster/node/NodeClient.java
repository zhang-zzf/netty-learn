package org.example.mqtt.broker.cluster.node;

import static org.example.mqtt.broker.cluster.node.Cluster.$_SYS_NODE_CLUSTER_MESSAGE_TOPIC_FILTER;
import static org.example.mqtt.broker.cluster.node.Cluster.$_SYS_NODE_TOPIC;
import static org.example.mqtt.broker.cluster.node.NodeMessage.ACTION_PUBLISH_FORWARD;
import static org.example.mqtt.broker.cluster.node.NodeMessage.ACTION_SESSION_CLOSE;
import static org.example.mqtt.broker.cluster.node.NodeMessage.INFO_CLIENT_CONNECT;
import static org.example.mqtt.broker.cluster.node.NodeMessage.INFO_CLUSTER_NODES;
import static org.example.mqtt.broker.cluster.node.NodeMessage.NodeInfo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.EventLoopGroup;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.client.AbstractClient;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.SubAck;

@Slf4j
public class NodeClient extends AbstractClient {

    private final Cluster cluster;
    private final Node remoteNode;

    // 集群级别 clientIdentifier 不重名
    private final static AtomicLong clientIdentifierCounter = new AtomicLong(0);

    public NodeClient(Node remoteNode, EventLoopGroup eventLoop, Cluster cluster) throws URISyntaxException, ExecutionException, InterruptedException {
        super(clusterClientIdentifier(cluster), new URI(remoteNode.address()), eventLoop);
        this.remoteNode = remoteNode;
        this.cluster = cluster;
        ConnAck connAck = this.connect(Connect.from(clientIdentifier(), keepAlive())).get();
        if (!connAck.connectionAccepted()) {
            throw new IllegalStateException("Connection not accepted");
        }
        subscribe(clientIdentifier(), Publish.EXACTLY_ONCE);
    }

    private static String clusterClientIdentifier(Cluster cluster) {
        return String.format($_SYS_NODE_TOPIC, cluster.broker().nodeId(), clientIdentifierCounter.getAndIncrement());
    }

    public CompletionStage<SubAck> subscribeClusterMessage() {
        CompletionStage<SubAck> future = subscribe($_SYS_NODE_CLUSTER_MESSAGE_TOPIC_FILTER, Publish.AT_LEAST_ONCE);
        future.exceptionally((e) -> {
            if (e != null) {
                logAndClose(e);
            }
            return null;
        });
        return future;
    }

    private ClusterBroker broker() {
        return cluster.broker();
    }

    @Override
    public void onPublish(Publish packet) {
        ByteBuf payload = packet.payload();
        NodeMessage m;
        if ((byte) '{' == payload.getByte(0)) {
            // json NodeMessage
            m = NodeMessage.fromBytes(ByteBufUtil.getBytes(payload));
        }
        else {
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
                log.warn("NodeClient receive unknown NodeMessage-> packetType: {}", m);
        }
    }

    private void doHandleActionSessionClose(NodeMessage m) {
        String clientIdentifier = m.unwrapSessionClose();
        ServerSession session = broker().session(clientIdentifier);
        log.info("NodeClient receive Session.Closed. cur Session->{}", session);
        if (session != null) {
            session.close();
            log.info("NodeClient Session.Closed->{}", clientIdentifier);
        }
        else {
            log.warn("NodeClient does not exist Session({})", clientIdentifier);
        }
    }

    private void doHandleInfoClusterNodes(NodeMessage m) {
        Set<NodeInfo> state = m.unwrapClusterNodes();
        log.debug("NodeClient receive Cluster.Nodes-> ncId: {}, remote: {}, remoteState: {}",
            clientIdentifier(), m.getNodeId(), state);
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
        }
    }

    @Override
    public void close() {
        super.close();
        log.info("NodeClient clientClosed -> cId: {}, id: {}, remote: {}", clientIdentifier(), remoteNode.id(), remoteNode.address());
        cluster.removeNodeClientFromNode(this, remoteNode);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (clientIdentifier() != null) {
            sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    private void logAndClose(Throwable e) {
        log.error("unExpected Exception", e);
        close();
    }

}
