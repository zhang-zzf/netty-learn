package org.example.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.model.Publish;

import java.util.Map;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toMap;
import static org.example.mqtt.broker.cluster.node.Node.NODE_ID_UNKNOWN;

@Slf4j
public class Cluster implements AutoCloseable {

    public static final String $_SYS_NODES_TOPIC = "$SYS/nodes/";
    public static final String $_SYS_CLUSTER_NODES_TOPIC = "$SYS/cluster/nodes";
    private final ConcurrentMap<String, Node> nodes = new ConcurrentHashMap<>();

    private final ClusterBroker clusterBroker;

    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> syncJob;

    public Cluster(ClusterBroker clusterBroker) {
        this.clusterBroker = clusterBroker;
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new DefaultThreadFactory("Cluster-Sync"));
    }

    public Cluster localNode(String listenedAddress) {
        nodes.putIfAbsent(nodeId(), new Node(nodeId(), listenedAddress));
        return this;
    }

    public Map<String, Node> nodes() {
        return nodes;
    }

    public void addNode(String nodeId, String remoteAddress) {
        Node firstJoinedNode = nodes.get(NODE_ID_UNKNOWN);
        if (firstJoinedNode != null && firstJoinedNode.address().equals(remoteAddress)) {
            log.info("Node({}) receive first joined node's nodeId: {}", nodeId);
            Node updateFirstNode = new Node(nodeId, remoteAddress).nodeClient(firstJoinedNode.nodeClient());
            nodes.put(nodeId, updateFirstNode);
            nodes.remove(NODE_ID_UNKNOWN);
            log.info("Node({}) cur Cluster: {}", nodes);
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
        NodeClient nc = connectNewNode(remoteAddress);
        if (nc == null) {
            // wait for next update
            nodes.remove(nodeId, nn);
            log.error("Cluster try connect to {}/{} failed", nodeId, remoteAddress);
        } else {
            nn.nodeClient(nc);
            log.info("Cluster add new Node: {}", nn);
        }
    }

    public void updateNodes(Map<String, String> nodeIdToAddress) {
        log.debug("Cluster updateNodes: {}", JSON.toJSONString(nodeIdToAddress));
        for (Map.Entry<String, String> e : nodeIdToAddress.entrySet()) {
            addNode(e.getKey(), e.getValue());
        }
    }

    private NodeClient connectNewNode(String address) {
        try {
            return new NodeClient(address, this);
        } catch (Exception e) {
            log.error("Node({" + nodeId() + "}) connect to another node failed", e);
            return null;
        }
    }

    public void startSyncJob() {
        if (syncJob != null) {
            return;
        }
        syncJob = scheduledExecutorService.scheduleAtFixedRate(() -> {
            Map<String, String> state = nodes.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, e -> e.getValue().address()));
            log.debug("Node({}) publish Cluster State: {}, {}", nodeId(), $_SYS_CLUSTER_NODES_TOPIC, state);
            NodeMessage nm = NodeMessage.wrapClusterState(nodeId(), state);
            Publish publish = Publish.outgoing(Publish.AT_MOST_ONCE, $_SYS_CLUSTER_NODES_TOPIC, nm.toByteBuf());
            clusterBroker.nodeBroker().forward(publish);
        }, 1, 1, TimeUnit.SECONDS);
    }

    private String nodeId() {
        return clusterBroker.nodeId();
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
        return clusterBroker;
    }

    public void join(String anotherNodeAddress) {
        NodeClient nc = connectNewNode(anotherNodeAddress);
        if (nc == null) {
            throw new IllegalArgumentException("can not join the Cluster with another Node: " + anotherNodeAddress);
        }
        Node node = new Node(NODE_ID_UNKNOWN, anotherNodeAddress).nodeClient(nc);
        this.nodes.put(NODE_ID_UNKNOWN, node);
        log.info("Node({}) join to another Node({}), now Cluster: {}", nodeId(), anotherNodeAddress, nodes);
    }

}
