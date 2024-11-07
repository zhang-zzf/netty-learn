package org.example.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.node.bootstrap.BrokerBootstrap;
import org.example.mqtt.model.Publish;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static org.example.mqtt.broker.cluster.node.Node.NODE_ID_UNKNOWN;
import static org.example.mqtt.broker.cluster.node.NodeMessage.wrapClusterNodes;
import static org.example.mqtt.broker.node.bootstrap.BrokerBootstrap.MQTT_SERVER_THREAD_NUM;

@Slf4j
@Component
public class Cluster implements AutoCloseable {

    /**
     * 集群中 Node 监听的 topic。
     * <p>每个 node 都有1~n个唯一的 topic, 用于接受发送到 Node 的消息</p>
     * <p>Example: $SYS/cluster/node1/0</p>
     * <p>Example: $SYS/cluster/node1/1</p>
     * <p>Example: $SYS/cluster/node1/2</p>
     * <p>Example: $SYS/cluster/node1/n</p>
     */
    public static final String $_SYS_NODE_TOPIC = "$SYS/cluster/%s/%s";
    /**
     * 集群中 Node 发布消息的 topic
     * <p>Example: Node1 会发布集群变动消息到 $SYS/cluster/node1/node</p>
     * <p>Example: Node1 会发布 Session 变动消息到 $SYS/cluster/node1/session</p>
     * <p>Example: Node2 会发布集群变动消息到 $SYS/cluster/node2/node</p>
     */
    public static final String $_SYS_NODE_PUBLISH_TOPIC = "$SYS/cluster/nodes/%s/%s";
    public static final String $_SYS_NODE_CLUSTER_MESSAGE_TOPIC_FILTER = "$SYS/cluster/nodes/+/+";
    public static final String $_SYS_TOPIC = "$SYS";

    public static final int CONNECT_FAILED_THRESHOLD = 100;

    private final ConcurrentMap<String, Node> nodes = new ConcurrentHashMap<>();

    /**
     * Broker <--- NodeClient(another Broker)
     * <p>其他节点到本节点的通道，用于本节点向其他节点发送消息（如 forward Publish）</p>
     */
    private final ConcurrentMap<String, Set<String>> channelsToOtherNodes = new ConcurrentHashMap<>();

    private ClusterBroker clusterBroker;

    private final AtomicBoolean started = new AtomicBoolean(false);
    /**
     * 集群同步定时任务
     */
    private ScheduledThreadPoolExecutor scheduledExecutorService;
    private volatile ScheduledFuture<?> syncJob;
    private int publishClusterNodesPeriod = Integer.getInteger("mqtt.server.cluster.node.sync.period", 1);

    /**
     * 单线程处理 Cluster 更新
     */
    private final ExecutorService executorService = new ThreadPoolExecutor(1, 1,
            60, TimeUnit.SECONDS,
            // 使用无界队列
            new LinkedBlockingDeque<>(),
            (r) -> new Thread(r, "cluster-update"),
            new ThreadPoolExecutor.AbortPolicy()
    );

    private static final int MQTT_CLUSTER_CLIENT_CHANNEL_NUM;

    static {
        MQTT_CLUSTER_CLIENT_CHANNEL_NUM = Integer.getInteger("mqtt.server.cluster.node.channel.num",
                MQTT_SERVER_THREAD_NUM * 2);
        log.info("MQTT_CLUSTER_CLIENT_CHANNEL_NUM-> {}", MQTT_CLUSTER_CLIENT_CHANNEL_NUM);
    }


    private final EventLoopGroup clientEventLoopGroup;

    public Cluster() {
        DefaultThreadFactory tf = new DefaultThreadFactory("cluster-client");
        clientEventLoopGroup = new NioEventLoopGroup(MQTT_SERVER_THREAD_NUM, tf);
        // init metric for nodes
        initMetricsForNodes();
    }

    public static String clusterNodeId(String nodeName) {
        return nodeName + "@" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    }

    public static String[] idToNodeNameAndTimestamp(String nodeId) {
        int idx = nodeId.lastIndexOf("@");
        return new String[]{
                nodeId.substring(0, idx),
                nodeId.substring(idx + 1)
        };
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
            log.info("Cluster receive first joined Node's nodeId-> remoteNodeId: {}", nodeId);
            if (nodes.remove(NODE_ID_UNKNOWN, firstJoinedNode)) {
                nodes.put(nodeId, firstJoinedNode.nodeId(nodeId));
                log.info("Cluster.Nodes-> num: {}, nodes: {}", nodes.size(), nodes.values());
            }
            // sync Cluster.Nodes immediately.
            publishClusterNodes();
        } else {
            addNode(nodeId, remoteAddress);
        }
    }

    @SneakyThrows
    private boolean addNode(String nodeId, String remoteAddress) {
        // node.nodeId Check
        Node node = nodes.get(nodeId);
        if (node != null) {
            if (!node.address().equals(remoteAddress)) {
                log.error("Cluster add Node failed, same nodeId with different remoteAddress->{}, {}", nodeId, remoteAddress);
                return false;
            }
            log.debug("Cluster add Node-> exist same Node: {}", nodeId);
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
        // 与 Broker 建立 n 个 NodeClient，用于接受从 Broker 转发的 Publish 消息
        buildChannelsToNode(nn);
        log.debug("Cluster buildChannelsToNode-> Node: {}", nn);
        if (nn.nodeClient() == null) {
            // wait for next update
            nodes.remove(nodeId, nn);
            log.error("Cluster connect to new Node failed-> Node: {}", nn);
            return false;
        } else {
            log.info("Cluster connected to new Node->{}", nn);
            log.info("Cluster.Nodes-> num: {}, nodes: {}", nodes.size(), nodes.values());
            // sync Cluster.Nodes immediately.
            publishClusterNodes();
            // node now is definitely online
            nodeMayOffline.remove(nodeId);
            return true;
        }
    }

    private void buildChannelsToNode(Node nn) {
        // no connection to this Node
        if (nn.getId().equals(nodeId())) {
            return;
        }
        // Why？why not just use a single NodeClient?
        // 性能问题。与 Broker 建立一条 tcp(nc) 可以处理的数据量有限，影响 Publish 消息在集群中转发的效率（增加了消息的延时）
        // 同一时间只有一个线程更新 Node 中的数据
        for (int i = nn.nodeClientsCnt(); i < MQTT_CLUSTER_CLIENT_CHANNEL_NUM; i++) {
            log.debug("Cluster try to build new Channel to Node-> remoteNode: {}, {}", nn.id(), nn.address());
            NodeClient nc = buildChannelToNode(nn);
            if (nc != null) {
                nn.addNodeClient(nc);
                log.info("Cluster built new Channel to Node-> local: {}, remoteNode: {}, {}", nc, nn.id(), nn.address());
            } else {
                // 判断已建立的连接是否可用
                nn.checkNodeClientAvailable();
            }
        }
    }

    public void updateNodes(String remoteNodeId, Set<NodeMessage.NodeInfo> nodeInfos) {
        if (broker().closed()) {
            log.debug("Broker was closed, ignore updateNodes");
            return;
        }
        executorService.submit(() -> {
            log.debug("Cluster before update-> nodes: {}, channels: {}", nodes, channelsToOtherNodes);
            for (NodeMessage.NodeInfo node : nodeInfos) {
                // update Node Info
                updateNode(node.getId(), node.getAddress());
                updateChannels(remoteNodeId, node);
            }
            log.debug("Cluster after update-> nodes: {}, channels: {}", nodes, channelsToOtherNodes);
        });
    }

    private void updateChannels(String remoteNodeId, NodeMessage.NodeInfo node) {
        // 只关心其他节点到我自己节点的连接信息
        if (!node.getId().equals(nodeId())) {
            return;
        }
        if (node.getNodeClientIds() == null) {
            return;
        }
        // 直接使用对方广播给我的信息
        channelsToOtherNodes.put(remoteNodeId, node.getNodeClientIds());
        // check Session exist
        for (String nodeClientId : node.getNodeClientIds()) {
            ServerSession session = clusterBroker.nodeBroker().session(nodeClientId);
            if (session == null) {
                log.error("Cluster updateChannel, No Session-> cId: {}", nodeClientId);
            }
        }
    }

    private NodeClient buildChannelToNode(Node remoteNode) {
        try {
            return new NodeClient(remoteNode, clientEventLoopGroup, this);
        } catch (Exception e) {
            log.error("Cluster buildChannelToNode failed-> remoteNode: {}", remoteNode);
            log.error("Cluster buildChannelToNode failed", e);
            // 连接节点失败，添加统计
            if (remoteNode.connectFailed() > CONNECT_FAILED_THRESHOLD) {
                log.error("Cluster buildChannelToNode failed more than {} times, " +
                        "now remove it from Cluster-> Node: {}", CONNECT_FAILED_THRESHOLD, remoteNode);
                boolean removed = nodes.remove(remoteNode.id(), remoteNode);
                if (!removed) {
                    log.error("Cluster remove Node failed-> Node: {}, Cluster: {}", remoteNode, JSON.toJSON(nodes));
                }
            }
            return null;
        }
    }

    private void startSyncJob() {
        if (started.get()) {
            return;
        }
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new DefaultThreadFactory("Cluster-Sync"));
        this.syncJob = scheduledExecutorService.scheduleAtFixedRate(() -> publishClusterNodes(),
                publishClusterNodesPeriod, publishClusterNodesPeriod, TimeUnit.SECONDS);
    }

    @SneakyThrows
    private void publishClusterNodes() {
        // 把本地状态更新到最新后发布本地状态到集群
        if (broker().closed()) {
            return;
        }
        executorService.submit(() -> {
            for (Node n : nodes.values()) {
                buildChannelsToNode(n);
            }
        });
        Set<NodeMessage.NodeInfo> state = nodes.values().stream()
                // filter out the UNKNOWN_NODE
                .filter(n -> !n.id().equals(NODE_ID_UNKNOWN))
                // filter out unhealthy node
                .filter(n -> n.id().equals(nodeId()) || n.nodeClientsCnt() == MQTT_CLUSTER_CLIENT_CHANNEL_NUM)
                .map(n -> toNodeInfo(n, channelsToOtherNodes)).collect(toSet());
        // 广播我的集群状态
        String publishTopic = nodePublishTopic(nodeId(), "node");
        log.debug("Cluster.publishClusterNodes() Cluster.Nodes-> nodes: {}, topic: {}", state, publishTopic);
        NodeMessage nm = wrapClusterNodes(nodeId(), state);
        Publish publish = Publish.outgoing(Publish.AT_LEAST_ONCE, publishTopic, nm.toByteBuf());
        int cnt = clusterBroker.nodeBroker().forward(publish);
        if (cnt != nodes.size() - 1) {
            Optional<Topic> topic = clusterBroker.nodeBroker().topic($_SYS_NODE_CLUSTER_MESSAGE_TOPIC_FILTER);
            log.warn("Cluster.publishClusterNodes() failed-> " +
                    "curNodesNum: {}, published: {}, topic: {}", nodes.size(), cnt, topic);
        }
        logNodeMetrics();
    }

    private void logNodeMetrics() {
        try {
            for (Map.Entry<String, Node> e : nodes.entrySet()) {
                if (NODE_ID_UNKNOWN.equals(e.getKey()) || nodeId().equals(e.getKey())) {
                    continue;
                }
                // broker.cluster.node.nodes
                logNodeMetric(e.getValue());
            }
            // 集群消息主题的订阅
            Optional<Topic> topicOpt = clusterBroker.nodeBroker().topic($_SYS_NODE_CLUSTER_MESSAGE_TOPIC_FILTER);
            MetricUtil.gauge("broker.cluster.node.topic.cm",
                    topicOpt.isPresent() ? topicOpt.get().subscribers().size() : 0,
                    "node", nodeId());
        } catch (Throwable e) {
            log.error("unExpected Exception", e);
        }
    }

    private void logNodeMetric(Node node) {
        String remoteNode = node.id();
        MetricUtil.gauge("broker.cluster.node.clients", node.nodeClientsCnt(),
                "node", nodeId(), "remoteNode", remoteNode);
        MetricUtil.gauge("broker.cluster.node.clients.cm", node.cmClient() == null ? 0 : 1,
                "node", nodeId(), "remoteNode", remoteNode);
        Set<String> subscribers = channelsToOtherNodes.get(remoteNode);
        MetricUtil.gauge("broker.cluster.node.subscribers",
                subscribers == null ? 0 : subscribers.size(),
                "node", nodeId(), "remoteNode", remoteNode);
    }

    private NodeMessage.NodeInfo toNodeInfo(Node n, ConcurrentMap<String, Set<String>> channelsToOtherNodes) {
        return new NodeMessage.NodeInfo()
                .setId(n.getId())
                .setAddress(n.getAddress())
                .setNodeClientIds(n.nodeClientIdSet())
                .setCmNodeClientId(ofNullable(n.cmClient()).map(NodeClient::getClientIdentifier).orElse(null))
                .setSubscribers(channelsToOtherNodes.get(n.getId()))
                ;
    }

    private String nodeId() {
        return clusterBroker.nodeId();
    }

    @Override
    public void close() throws Exception {
        log.info("Cluster.close()");
        cancelSyncJob();
        closeAllClient();
        executorService.shutdownNow();
    }

    private void cancelSyncJob() {
        if (syncJob != null) {
            syncJob.cancel(true);
            syncJob = null;
            scheduledExecutorService.shutdown();
            log.info("Cluster.cancelSyncJob()");
        }
    }

    private void closeAllClient() throws Exception {
        for (Map.Entry<String, Node> e : nodes.entrySet()) {
            if (e.getValue().getId().equals(nodeId())) {
                continue;
            }
            e.getValue().close();
        }
        // shutdown the threads
        clientEventLoopGroup.shutdownGracefully();
        log.info("Cluster.closeAllClient()");
    }

    public Node node(String nodeId) {
        return nodes.get(nodeId);
    }

    public ClusterBroker broker() {
        return clusterBroker;
    }

    @SneakyThrows
    public void join(String anotherNodeAddress) {
        if (!started.get()) {
            throw new IllegalStateException("Cluster is not started yet.");
        }
        // 更新 Cluster 内部数据的操作交给单线程处理
        executorService.submit(() -> {
            boolean nodeAdded = addNode(NODE_ID_UNKNOWN, anotherNodeAddress);
            if (nodeAdded) {
                Set<NodeMessage.NodeInfo> localNode = localNodeInfo();
                log.info("Cluster start sync Local Node with remote Node-> local: {}, remote: {}", localNode, anotherNodeAddress);
                NodeMessage nm = wrapClusterNodes(broker().nodeId(), localNode);
                // no need to wait for ack
                nodes.get(NODE_ID_UNKNOWN).nodeClient()
                        .sendAsync(Publish.AT_LEAST_ONCE, $_SYS_TOPIC, nm.toByteBuf());
                log.info("Cluster sync done");
            }
        }).get();
    }

    private HashSet<NodeMessage.NodeInfo> localNodeInfo() {
        Node localNode = nodes.get(broker().nodeId());
        if (localNode == null) {
            throw new IllegalStateException("Cluster does not have a LocalNode, use join to add Local ClusterBroker");
        }
        return new HashSet<NodeMessage.NodeInfo>() {{
            add(toNodeInfo(localNode, channelsToOtherNodes));
        }};
    }

    public Cluster bind(ClusterBroker clusterBroker) {
        this.clusterBroker = clusterBroker;
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
        log.info("Cluster started, Cluster.Nodes->{}", nodes.values());
        started.set(true);
    }

    public void removeNode(Node node) {
        executorService.submit(() -> {
            log.info("Cluster remove Node->{}", node);
            log.info("Cluster.Nodes before remove->{}", nodes.values());
            nodes.remove(node.id(), node);
            channelsToOtherNodes.remove(node.id());
            log.info("Cluster.Nodes after remove->{}", nodes.values());
            logNodeMetric(node);
        });
    }

    public static String sessionChangePublishTopic(String localNodeId) {
        return nodePublishTopic(localNodeId, "session");
    }

    public static String nodePublishTopic(String localNodeId, String type) {
        return String.format($_SYS_NODE_PUBLISH_TOPIC, localNodeId, type);
    }

    private final ConcurrentMap<String, Long> nodeMayOffline = new ConcurrentHashMap<>();
    private final long nodeOfflinePeriod = Long.getLong("mqtt.server.cluster.node.offline.period", 300000);

    public boolean checkNodeOnline(String nodeId) {
        String[] nameAndTime = idToNodeNameAndTimestamp(nodeId);
        for (String nId : nodes.keySet()) {
            if (nId.startsWith(nameAndTime[0])) {
                Long tTime = Long.valueOf(nameAndTime[1]);
                Long curTime = Long.valueOf(idToNodeNameAndTimestamp(nId)[1]);
                if (tTime < curTime) {
                    log.info("checkNodeOnline result-> {} is offline, reason: new same name Node in Cluster", nodeId);
                    return false;
                } else if (tTime.equals(curTime)) {
                    // watch out: if (Long == Long) {}
                    return true;
                } else {
                    log.error("checkNodeOnline result-> {} nodeId is illegal", nodeId);
                    return false;
                }
            }
        }
        // not found in Cluster
        Long timestamp = nodeMayOffline.get(nodeId);
        if (timestamp == null) {
            timestamp = System.currentTimeMillis();
            nodeMayOffline.put(nodeId, timestamp);
        }
        if (System.currentTimeMillis() - timestamp >= nodeOfflinePeriod) {
            log.info("checkNodeOnline result-> {} is offline, reason: node was offline after {}ms", nodeId, nodeOfflinePeriod);
            return false;
        }
        // 默认在线
        return true;
    }

    @Nullable
    public String pickOneChannelToNode(String targetNodeId) {
        Set<String> set = channelsToOtherNodes.get(targetNodeId);
        if (set == null) {
            return null;
        }
        return randomPick(set);
    }

    private <T> T randomPick(Set<T> set) {
        int size = set.size();
        int random = ThreadLocalRandom.current().nextInt(size);
        int i = 0;
        for (T s : set) {
            if (i == random) {
                return s;
            }
            i += 1;
        }
        // never go here
        throw new IllegalStateException();
    }

    @NotNull
    public Set<String> channelsToNode(String targetNodeId) {
        return ofNullable(channelsToOtherNodes.get(targetNodeId)).orElse(emptySet());
    }

    public void removeNodeClientFromNode(NodeClient nodeClient, Node remoteNode) {
        Runnable task = () -> {
            remoteNode.removeNodeClient(nodeClient);
            log.info("NodeClient was removed from Node-> client: {}, Node: {}", nodeClient, remoteNode);
            if (remoteNode.nodeClientsCnt() == 0) {
                removeNode(remoteNode);
            }
        };
        // 单线程处理 Cluster 集群状态
        executorService.submit(task);
    }

}
