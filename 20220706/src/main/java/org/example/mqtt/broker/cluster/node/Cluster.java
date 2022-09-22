package org.example.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.model.Publish;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.toMap;
import static org.example.mqtt.broker.cluster.node.Node.NODE_ID_UNKNOWN;

@Slf4j
public class Cluster implements AutoCloseable {

    public static final String $_SYS_NODES_TOPIC = "$SYS/nodes/";
    public static final String $_SYS_CLUSTER_NODES_TOPIC = "$SYS/cluster/nodes";
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();

    private ClusterBroker localBroker;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private ScheduledThreadPoolExecutor scheduledExecutorService;
    private ScheduledFuture<?> syncJob;

    public Cluster() {
    }

    public Map<String, Node> nodes() {
        return nodes;
    }

    public void addNode(String nodeId, String remoteAddress) {
        Node firstJoinedNode = nodes.get(NODE_ID_UNKNOWN);
        if (firstJoinedNode != null && firstJoinedNode.address().equals(remoteAddress)) {
            log.info("Cluster receive first joined node's nodeId: {}", nodeId);
            Node updateFirstNode = new Node(nodeId, remoteAddress).nodeClient(firstJoinedNode.nodeClient());
            nodes.put(nodeId, updateFirstNode);
            nodes.remove(NODE_ID_UNKNOWN);
            log.info("Cluster.Nodes->{}", nodes);
            return;
        }
        if (nodes.get(nodeId) != null) {
            if (!nodes.get(nodeId).address().equals(remoteAddress)) {
                log.error("Cluster add Node failed, same nodeId with different remoteAddress", nodeId, remoteAddress);
            }
            return;
        }
        Node nn = new Node(nodeId, remoteAddress);
        if (nodes.putIfAbsent(nodeId, nn) != null) {
            log.warn("Cluster add Node failed, may be other Thread add it.", nodeId, remoteAddress);
            return;
        }
        // try to connect the Node
        NodeClient nc = connectNewNode(nn);
        if (nc == null) {
            // wait for next update
            nodes.remove(nodeId, nn);
            log.error("Cluster try connect to {}/{} failed", nodeId, remoteAddress);
        } else {
            nn.nodeClient(nc);
            log.info("Cluster add new Node->{}", nn);
            log.info("Cluster.Nodes->{}", JSON.toJSONString(nodes));
        }
    }

    public void updateNodes(Map<String, String> nodeIdToAddress) {
        log.debug("Cluster before update: {}", nodes);
        for (Map.Entry<String, String> e : nodeIdToAddress.entrySet()) {
            addNode(e.getKey(), e.getValue());
        }
        log.debug("Cluster after update: {}", nodes);
    }

    private NodeClient connectNewNode(Node node) {
        try {
            return new NodeClient(node, this);
        } catch (Exception e) {
            log.error("Cluster connect to another node failed", e);
            return null;
        }
    }

    private void startSyncJob() {
        if (started.get()) {
            return;
        }
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new DefaultThreadFactory("Cluster-Sync"));
        syncJob = scheduledExecutorService.scheduleAtFixedRate(() -> {
            Map<String, String> state = nodes.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, e -> e.getValue().address()));
            log.trace("Node({}) publish Cluster State: {}, {}", nodeId(), $_SYS_CLUSTER_NODES_TOPIC, state);
            NodeMessage nm = NodeMessage.wrapClusterState(nodeId(), state);
            Publish publish = Publish.outgoing(Publish.AT_MOST_ONCE, $_SYS_CLUSTER_NODES_TOPIC, nm.toByteBuf());
            // publish to topicName: $SYS/cluster/nodes
            localBroker.nodeBroker().forward(publish);
        }, 16, 16, TimeUnit.SECONDS);
    }

    private String nodeId() {
        return localBroker.nodeId();
    }

    @Override
    public void close() throws Exception {
        syncJob.cancel(true);
        syncJob = null;
        scheduledExecutorService.shutdown();
    }

    public Node node(String nodeId) {
        return nodes.get(nodeId);
    }

    public ClusterBroker broker() {
        return localBroker;
    }

    public void join(String anotherNodeAddress) {
        if (!started.get()) {
            throw new IllegalStateException("Cluster is not started yet.");
        }
        Node node = new Node(NODE_ID_UNKNOWN, anotherNodeAddress);
        NodeClient nc = connectNewNode(node);
        if (nc == null) {
            throw new IllegalArgumentException("can not join the Cluster with another Node: " + anotherNodeAddress);
        }
        nodes.put(NODE_ID_UNKNOWN, node.nodeClient(nc));
        Map<String, String> localNode = localNodeInfo();
        nc.syncLocalNode(localNode);
        log.info("Cluster sync Local Node({}) with remote Node({})", localNode, node);
    }

    private Map<String, String> localNodeInfo() {
        String localNodeId = broker().nodeId();
        Node localNode = nodes.get(localNodeId);
        if (localNode == null) {
            throw new IllegalStateException("Cluster does not have a LocalNode, use join to add Local ClusterBroker");
        }
        return new HashMap<String, String>(4) {{
            put(localNodeId, localNode.address());
        }};
    }

    public Cluster join(ClusterBroker clusterBroker) {
        clusterBroker.join(this);
        this.localBroker = clusterBroker;
        return this;
    }

    public void start() {
        if (started.get()) {
            return;
        }
        String mqttUrl = localBroker.listenedServer().get("mqtt");
        if (mqttUrl == null) {
            throw new UnsupportedOperationException("Cluster mode need mqtt protocol enabled");
        }
        nodes.putIfAbsent(nodeId(), new Node(nodeId(), mqttUrl));
        startSyncJob();
        log.info("Cluster started, Cluster.Nodes->{}", JSON.toJSONString(nodes));
        started.set(true);
    }

    public void removeNode(Node node) {
        log.info("Cluster remove Node->{}", node);
        log.info("Cluster.Nodes before remove->{}", JSON.toJSONString(nodes));
        if (!nodes.remove(node.id(), node)) {
            log.error("Cluster.Nodes remove node failed");
        }
        log.info("Cluster.Nodes after remove->{}", JSON.toJSONString(nodes));
    }

}
