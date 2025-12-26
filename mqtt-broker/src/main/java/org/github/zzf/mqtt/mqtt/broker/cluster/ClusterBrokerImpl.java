package org.github.zzf.mqtt.mqtt.broker.cluster;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.github.zzf.mqtt.mqtt.broker.cluster.node.Cluster.sessionChangePublishTopic;
import static org.github.zzf.mqtt.mqtt.broker.cluster.node.NodeMessage.ACTION_BROKER_CLOSE;
import static org.github.zzf.mqtt.mqtt.broker.cluster.node.NodeMessage.ACTION_SESSION_CLOSE;
import static org.github.zzf.mqtt.mqtt.broker.cluster.node.NodeMessage.ACTION_TOPIC_QUERY;
import static org.github.zzf.mqtt.mqtt.broker.cluster.node.NodeMessage.INFO_CLUSTER_NODES;
import static org.github.zzf.mqtt.server.DefaultBroker.qoS;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Status.INIT;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Type.OUT;

import io.micrometer.core.annotation.Timed;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.micrometer.utils.MetricUtil;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.ServerSession;
import org.github.zzf.mqtt.protocol.server.Topic;
import org.github.zzf.mqtt.mqtt.broker.cluster.node.Cluster;
import org.github.zzf.mqtt.mqtt.broker.cluster.node.NodeMessage;
import org.github.zzf.mqtt.mqtt.broker.cluster.node.NodePublish;
import org.github.zzf.mqtt.bootstrap.BrokerBootstrap;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe;

@Slf4j
public class ClusterBrokerImpl implements ClusterBroker, Broker {

    private final ClusterBrokerState clusterBrokerState;
    private final Broker nodeBroker;
    private final Cluster cluster;

    /**
     * <pre>
     * 0 ->open
     * 1 ->closing
     * 2 ->closed
     * </pre>
     */
    private final AtomicInteger closeStatus = new AtomicInteger(0);

    /**
     * JVM 级别
     */
    private static final String nodeId;

    static {
        String nodeName = System.getProperty("mqtt.server.cluster.nodeName", "node");
        log.info("nodeName-> {}", nodeName);
        if (nodeName.indexOf("/") > 0) {
            log.info("nodeName contains '/', now replace it with '_'");
            nodeName = nodeName.replaceAll("/", "_");
            log.info("nodeName-> {}", nodeName);
        }
        nodeId = Cluster.clusterNodeId(nodeName);
        log.info("Broker.nodeId->{}", nodeId);
    }

    public ClusterBrokerImpl(ClusterBrokerState clusterBrokerState, Broker nodeBroker, Cluster cluster) {
        this.clusterBrokerState = clusterBrokerState;
        this.nodeBroker = nodeBroker;
        this.cluster = cluster;
        // todo
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdownGracefully();
            } catch (Exception e) {
                log.error("unExpected exception", e);
            }
        }, "ClusterBrokerShutdown"));
    }

    /**
     * Broker join the Cluster
     */
    public Cluster cluster() {
        return this.cluster;
    }

    private void publishConnectToCluster(String clientIdentifier) {
        NodeMessage nm = NodeMessage.wrapConnect(nodeId(), clientIdentifier);
        Publish packet = Publish.outgoing(1, sessionChangePublishTopic(nodeId()), nm.toByteBuf());
        nodeBroker.forward(packet);
        log.debug("Client({}).Connect was published to the Cluster", clientIdentifier);
    }

    /**
     * 清理路由表
     * <p>异步清理</p>
     * <p>即使清理失败也可以接受，{@link ClusterBrokerImpl#forward(Publish)}中有兜底清理策略</p>
     */
    private CompletableFuture<Void> removeNodeFromTopicAsync(Set<Subscribe.Subscription> subscriptions) {
        List<String> topicToRemove = subscriptions.stream()
                .map(Subscribe.Subscription::topicFilter)
                // todo
                // .filter(topicFilter -> nodeBroker.topic(topicFilter).isEmpty())
                .collect(toList());
        return clusterBrokerState.removeNodeFromTopicAsync(nodeId(), topicToRemove);
    }

    @Override
    public int forward(Publish packet) {
        int times = nodeBroker.forward(packet);
        // must retain the Publish.packet for async callback
        // todo
        // packet.retain();
        clusterBrokerState.matchTopicAsync(packet.topicName())
                .thenAccept((topics) -> forwardToBrokerAndOfflineSession(packet, topics))
                .whenComplete((v, t) -> {
                    // must release PublishPacket anyway
                    // todo
                    // packet.release();
                    if (t != null) {
                        log.error("forward Publish failed-> Publish: {}", packet);
                        log.error("unExpected Exception", t);
                    }
                });
        return times;
    }

    @Override
    public List<Subscription> subscribe(ServerSession session, Collection<Subscription> subscriptions) {
        return List.of();
    }

    @Override
    public void unsubscribe(ServerSession session, Collection<Subscription> subscriptions) {

    }

    private void forwardToBrokerAndOfflineSession(Publish packet, List<ClusterTopic> clusterTopics) {
        // 从 DB 获取到 ClusterTopic 后的异步回调
        log.debug("Publish({}) match Cluster Topics: {}", packet.topicName(), clusterTopics);
        if (clusterTopics.isEmpty()) {
            return;
        }
        // todo Set 替代方案
        Set<String> sendNodes = new HashSet<>(4);
        for (ClusterTopic ct : clusterTopics) {
            // forward to another Node in the cluster
            for (String targetNodeId : ct.getNodes()) {
                // 一个 Node 只转发一次
                if (!sendNodes.add(targetNodeId)) {
                    continue;
                }
                if (nodeId().equals(targetNodeId)) {
                    // Node local forward
                    // times += nodeBroker.forward(packet);
                } else {
                    forwardToOtherNode(packet, ct, targetNodeId);
                }
            }
            // forward the PublishPacket to offline client's Session.
            for (Map.Entry<String, Integer> e : ct.getOfflineSessions().entrySet()) {
                forwardToOfflineSession(packet, ct.topicFilter(), e.getKey(), e.getValue());
            }
        }
    }

    private void forwardToOtherNode(Publish packet, ClusterTopic ct, String targetNodeId) {
        boolean forwarded = false;
        // 先随机挑一个通道转发
        String topicName = cluster.pickOneChannelToNode(targetNodeId);
        if (topicName != null) {
            forwarded = doForwardToOtherNode(packet, targetNodeId, topicName);
            if (forwarded) {
                return;
            }
        }
        // 兜底遍历所有通道转发
        Set<String> clientIdentifiers = cluster.channelsToNode(targetNodeId);
        for (String cId : clientIdentifiers) {
            forwarded = doForwardToOtherNode(packet, targetNodeId, cId);
            // should forward the packet only once;
            if (forwarded) {
                break;
            }
        }
        if (!forwarded) {
            removeNodeFromTopicIfNodeOffline(ct, targetNodeId);
        }
    }

    private boolean doForwardToOtherNode(Publish packet, String targetNodeId, String topicName) {
        NodePublish nm = NodePublish.wrapPublish(nodeId(), packet);
        // nodeMessagePacket.payload 指向 CompositeByteBuf, 而不是指向 packet 的 ByteBuf
        // important: nm.retainPublishPacket() must be called before convert to CompositeByteBuf
        // when nm.ByteBuf released, the retainedPublishPacket will also be released.
        ByteBuf payload = nm.retainPublishPacket().toByteBuf();
        try {
            Publish nodeMessagePacket = Publish.outgoing(packet.qos(), topicName, payload);
            log.debug("forward Publish to other Node-> Node: {}, through topic: {}", targetNodeId, topicName);
            int times = nodeBroker.forward(nodeMessagePacket);
            if (times > 0) {
                logMetrics(nm, targetNodeId);
                return true;
            }
            return false;
        } finally {
            payload.release();
        }
    }

    private void removeNodeFromTopicIfNodeOffline(ClusterTopic ct, String targetNodeId) {
        // target node may be permanently closed or temporary offline
        if (!cluster.checkNodeOnline(targetNodeId)) {
            // 注意 cluster.nodes 打印 log 时可能非常庞大。Example: 13 Node * 64 Channel = 832 个 NodeClient
            log.info("removeNodeFromTopic->nodeId: {}, topic: {}, curCluster: {}", targetNodeId, ct.topicFilter(), cluster.nodes().keySet());
            // 移除路由表
            clusterBrokerState.removeNodeFromTopicAsync(targetNodeId, singletonList(ct.topicFilter()));
        }
    }

    public static final String METRIC_NAME = "broker.cluster.ClusterBrokerImpl";

    private void logMetrics(NodeMessage nm, String targetNodeId) {
        try {
            // Publish.Receive <-> forward to another Broker
            MetricUtil.count(METRIC_NAME, 1,
                    "phase", "packetReceive->forwardToBroker",
                    "source", nodeId(),
                    "target", targetNodeId
            );
        } catch (Exception e) {
            log.error("unExpected Exception", e);
        }
    }

    private void forwardToOfflineSession(Publish packet, String tf, String cId, int qos) {
        // decide QoS
        qos = qoS(packet.qos(), qos);
        if (qos == Publish.AT_MOST_ONCE) {
            log.warn("forwardToOfflineSession do nothing, Publish.atMostOne-> {}", cId);
            return;
        }
        ClusterServerSession s = clusterBrokerState.getSession(cId);
        if (s == null) {
            log.warn("forwardToOfflineSession failed, Session not exist -> {}", cId);
            return;
        }
        if (s.isOnline()) {
            log.warn("forwardToOfflineSession failed, Session is online -> {}", cId);
            return;
        }
        // use a shadow copy of the origin Publish
        // todo
        Publish outgoing = null; //Publish.outgoing(packet, tf, (byte) qos, packetIdentifier(s, qos));
        ClusterControlPacketContext cpx =
                new ClusterControlPacketContext(clusterBrokerState, cId, OUT, outgoing, INIT, null);
        clusterBrokerState.offerCpx(null, cpx);
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public void close() throws Exception {
        log.info("Broker now close itself.");
        // no more new connections
        // org.example.mqtt.broker.cluster.ClusterServerSessionHandler.channelActive will read this flag
        if (!closeStatus.compareAndSet(0, 1)) {
            log.info("Broker is closing.");
            return;
        }
        log.info("Broker now try to close all Session");
        closeAllSession();
        // close the Cluster
        log.info("Broker now try to shutdown Cluster");
        cluster.close();
        // shutdown Broker
        log.info("Broker now try to shutdown Bootstrap");
        BrokerBootstrap.shutdownServer();
        log.info("Broker now try to shutdown ClusterDbRepo");
        clusterBrokerState.close();
        // shutdown the nodeBroker
        nodeBroker.close();
        closeStatus.compareAndSet(1, 2);
        log.info("Broker is closed.");
    }

    private void closeAllSession() {
        // // todo
        // for (Map.Entry<String, ServerSession> e : nodeBroker.sessionMap().entrySet()) {
        //     // todo
        //     // e.getValue().close();
        //     log.info("closeAllSession-> {}", e.getValue());
        // }
    }

    @Override
    public Broker nodeBroker() {
        return nodeBroker;
    }

    @Override
    public ClusterBrokerState state() {
        return clusterBrokerState;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"nodeId\":\"").append(nodeId()).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    @Timed(value = METRIC_NAME, histogram = true)
    public void onPublish(Publish packet) {
        // $SYS/# 特殊处理. 发送给 Broker 的 $SYS/# 消息，不做转发
        if (packet.topicName().startsWith("$SYS")) {
            doHandleSysPublish(packet);
            return;
        }
        // retain message
        if (packet.retainFlag()) {
            // todo
            throw new UnsupportedOperationException();
        }
        // Broker forward Publish to relative topic after receive a PublishPacket
        forward(packet);
    }

    private void doHandleSysPublish(Publish packet) {
        log.info("Broker receive SysPublish->{}", packet);
        NodeMessage m = NodeMessage.fromBytes(packet.payload());
        switch (m.getPacket()) {
            case INFO_CLUSTER_NODES -> {
                Set<NodeMessage.NodeInfo> state = m.unwrapClusterNodes();
                log.info("Broker receive Cluster.Nodes->{}", state);
                cluster.updateNodes(m.getNodeId(), state);
            }
            case ACTION_BROKER_CLOSE -> {
                log.info("Broker receive shutdown Instruction");
                shutdownGracefully();
            }
            case ACTION_SESSION_CLOSE -> doHandleActionSessionClose(m);
            case ACTION_TOPIC_QUERY -> doHandleActionTopicQuery(m);
            default ->
                    log.error("Broker receive Unknown Instruction->{}, {}", m, ByteBufUtil.prettyHexDump(packet.payload()));
        }
    }

    private void doHandleActionTopicQuery(NodeMessage m) {
        String topicFilter = m.unwrapSessionClose();
        // todo
        //Optional<Topic> topic = nodeBroker.topic(topicFilter);
        Optional<Topic> topic = Optional.empty();
        log.info("ClusterBroker receive Topic.Query-> tf: {}, topic: {}", topicFilter, topic);
        if (topic.isPresent()) {
            // $SYS/cluster/nodes/%s/%s
            String publishTopic = "$SYS/cluster/nodes/" + nodeId + "/query/result";
            NodeMessage nm = new NodeMessage().setNodeId(nodeId()).setPacket("Topic.Query.Result")
                    .setPayload(topic.get().toString());
            Publish publish = Publish.outgoing(Publish.AT_LEAST_ONCE, publishTopic, nm.toByteBuf());
            forward(publish);
        }
    }

    private void doHandleActionSessionClose(NodeMessage m) {
        String clientIdentifier = m.unwrapSessionClose();
        //todo
        // ServerSession session = nodeBroker.session(clientIdentifier);
        ServerSession session = null;
        log.info("ClusterBroker receive Session.Closed-> cId: {}, session: {}", clientIdentifier, session);
        if (session != null) {
            // todo
            // session.close();
            log.info("NodeClient Session.Closed->{}", clientIdentifier);
        } else {
            log.warn("NodeClient does not exist Session({})", clientIdentifier);
        }
    }

    @SneakyThrows
    public void shutdownGracefully() {
        close();
    }

    @Override
    public ServerSession onConnect(Connect connect, Channel channel) {
        // todo
        return null;
    }

}
