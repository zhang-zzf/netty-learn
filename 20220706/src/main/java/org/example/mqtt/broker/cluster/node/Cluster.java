package org.example.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.node.bootstrap.BrokerBootstrap;
import org.example.mqtt.model.Publish;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.toMap;
import static org.example.mqtt.broker.cluster.node.Node.NODE_ID_UNKNOWN;

@Slf4j
@Component
public class Cluster implements AutoCloseable {

    /**
     * 集群中 Node 监听的 topic。
     * <p>每个 node 都有唯一的 topic, 用于接受发送到 Node 的消息</p>
     * <p>Example: $SYS/cluster/node1</p>
     */
    public static final String $_SYS_NODE_TOPIC = "$SYS/cluster/nodes/%s";
    /**
     * 集群中 Node 发布消息的 topic
     * <p>Example: Node1 会发布集群变动消息到 $SYS/cluster/node1/node</p>
     * <p>Example: Node1 会发布 Session 变动消息到 $SYS/cluster/node1/session</p>
     * <p>Example: Node2 会发布集群变动消息到 $SYS/cluster/node2/node</p>
     */
    public static final String $_SYS_NODE_PUBLISH_TOPIC = "$SYS/cluster/nodes/%s/%s";
    public static final String $_SYS_TOPIC = "$SYS";
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();

    private ClusterBroker localBroker;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private ScheduledThreadPoolExecutor scheduledExecutorService;
    private volatile ScheduledFuture<?> syncJob;

    private int publishClusterNodesPeriod = Integer.getInteger("mqtt.server.cluster.node.sync.period", 300);

    public Cluster() {
        // init metric for nodes
        initMetricsForNodes();
    }

    private void initMetricsForNodes() {
        MetricUtil.gauge("broker.cluster.node.nodes", nodes);
    }

    public Map<String, Node> nodes() {
        return nodes;
    }

    private void updateNode(String nodeId, String remoteAddress) {
        Node firstJoinedNode = nodes.get(NODE_ID_UNKNOWN);
        if (firstJoinedNode != null && firstJoinedNode.address().equals(remoteAddress)) {
            log.debug("Cluster receive first joined Node's nodeId->{}", nodeId);
            if (nodes.remove(NODE_ID_UNKNOWN, firstJoinedNode)) {
                nodes.put(nodeId, firstJoinedNode.nodeId(nodeId));
            }
        } else {
            addNode(nodeId, remoteAddress);
        }
    }

    @SneakyThrows
    public boolean addNode(String nodeId, String remoteAddress) {
        // node.nodeId Check
        if (nodes.get(nodeId) != null) {
            if (!nodes.get(nodeId).address().equals(remoteAddress)) {
                log.error("Cluster add Node failed, same nodeId with different remoteAddress->{}, {}", nodeId, remoteAddress);
                return false;
            }
            return true;
        }
        // node.address Check
        for (Map.Entry<String, Node> e : nodes.entrySet()) {
            if (e.getValue().address().equals(remoteAddress)) {
                if (!e.getKey().equals(nodeId)) {
                    log.error("Cluster add Node failed, same remoteAddress with different nodeId->{}, {}", nodeId, remoteAddress);
                    return false;
                }
                return true;
            }
        }

        Node nn = new Node(nodeId, remoteAddress);
        if (nodes.putIfAbsent(nodeId, nn) != null) {
            log.warn("Cluster add Node failed, may be other Thread add it->{}, {}", nodeId, remoteAddress);
            return true;
        }
        log.info("Cluster now try to connect to new Node->{}", nn);
        // try to connect the Node
        NodeClient nc = connectNewNode(nn);
        if (nc == null) {
            // wait for next update
            nodes.remove(nodeId, nn);
            log.error("Cluster connect to new Node failed->{}", nn);
            return false;
        } else {
            nn.nodeClient(nc);
            log.info("Cluster connected to new Node->{}", nn);
            log.info("Cluster.Nodes-> num: {}, nodes: {}", nodes.size(), JSON.toJSONString(nodes.values()));
            // sync Cluster.Nodes immediately.
            publishClusterNodes();
            return true;
        }
    }

    public void updateNodes(Map<String, String> nodeIdToAddress) {
        log.debug("Cluster before update: {}", nodes);
        for (Map.Entry<String, String> e : nodeIdToAddress.entrySet()) {
            updateNode(e.getKey(), e.getValue());
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
        this.syncJob = scheduledExecutorService.scheduleAtFixedRate(() -> publishClusterNodes(),
                16, publishClusterNodesPeriod, TimeUnit.SECONDS);
    }

    private void publishClusterNodes() {
        Map<String, String> state = nodes.entrySet().stream()
                .filter(e -> !e.getKey().equals(NODE_ID_UNKNOWN))
                .collect(toMap(Map.Entry::getKey, e -> e.getValue().address()));
        String publishTopic = clusterNodeChangePublishTopic(nodeId());
        log.debug("Cluster publish Cluster.Nodes:{}->{}", state, publishTopic);
        NodeMessage nm = NodeMessage.wrapClusterNodes(nodeId(), state);
        Publish publish = Publish.outgoing(Publish.AT_LEAST_ONCE, publishTopic, nm.toByteBuf());
        localBroker.nodeBroker().forward(publish);
    }

    private String nodeId() {
        return localBroker.nodeId();
    }

    @Override
    public void close() throws Exception {
        cancelSyncJob();
        closeAllClient();
    }

    private void cancelSyncJob() {
        if (syncJob != null) {
            syncJob.cancel(true);
            syncJob = null;
            scheduledExecutorService.shutdown();
        }
    }

    private void closeAllClient() throws Exception {
        for (Map.Entry<String, Node> e : nodes.entrySet()) {
            if (e.getValue().getId().equals(nodeId())) {
                continue;
            }
            e.getValue().nodeClient().close();
        }
    }

    public Node node(String nodeId) {
        return nodes.get(nodeId);
    }

    public ClusterBroker broker() {
        return localBroker;
    }

    @SneakyThrows
    public void join(String anotherNodeAddress) {
        if (!started.get()) {
            throw new IllegalStateException("Cluster is not started yet.");
        }
        boolean nodeAdded = addNode(NODE_ID_UNKNOWN, anotherNodeAddress);
        if (nodeAdded) {
            Map<String, String> localNode = localNodeInfo();
            log.info("Cluster start sync Local Node with remote Node({})->{}", anotherNodeAddress, localNode);
            nodes.get(NODE_ID_UNKNOWN).nodeClient().syncLocalNode(localNode);
            log.info("Cluster sync done");
        }
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
        String mqttUrl = BrokerBootstrap.LISTENED_SERVERS.get("mqtt").getUrl();
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
        nodes.remove(node.id(), node);
        log.info("Cluster.Nodes after remove->{}", JSON.toJSONString(nodes));
    }

    public static String nodeListenTopic(String localNodeId) {
        return String.format($_SYS_NODE_TOPIC, localNodeId);
    }

    public static String sessionChangePublishTopic(String localNodeId) {
        return nodePublishTopic(localNodeId, "session");
    }

    public static String clusterNodeChangePublishTopic(String localNodeId) {
        return nodePublishTopic(localNodeId, "node");
    }

    public static String nodePublishTopic(String localNodeId, String type) {
        return String.format($_SYS_NODE_PUBLISH_TOPIC, localNodeId, type);
    }

    public static String subscribeAllNode() {
        return String.format($_SYS_NODE_PUBLISH_TOPIC, "+", "+");
    }

}
