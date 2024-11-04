package org.example.mqtt.broker.cluster;

import io.micrometer.core.annotation.Timed;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.cluster.node.NodeMessage;
import org.example.mqtt.broker.cluster.node.NodePublish;
import org.example.mqtt.broker.node.bootstrap.BrokerBootstrap;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.example.mqtt.broker.cluster.node.Cluster.sessionChangePublishTopic;
import static org.example.mqtt.broker.cluster.node.NodeMessage.*;
import static org.example.mqtt.broker.node.DefaultBroker.packetIdentifier;
import static org.example.mqtt.broker.node.DefaultBroker.qoS;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

@Slf4j
@Component
public class ClusterBrokerImpl implements ClusterBroker {

    private final ClusterDbRepo clusterDbRepo;
    private final Broker nodeBroker;
    private Cluster cluster;

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

    public ClusterBrokerImpl(ClusterDbRepo clusterDbRepo, Broker nodeBroker) {
        this.clusterDbRepo = clusterDbRepo;
        this.nodeBroker = nodeBroker;
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
    @Override
    public void join(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Broker join the Cluster
     */
    public Cluster cluster() {
        return this.cluster;
    }

    @Override
    @Nullable
    public ServerSession session(String clientIdentifier) {
        var localSession = nodeBroker.session(clientIdentifier);
        log.debug("Client({}) find session in LocalNode: {}", clientIdentifier, localSession);
        if (localSession != null) {
            return localSession;
        }
        var session = clusterDbRepo.getSession(clientIdentifier);
        log.debug("Client({}) find session in Cluster: {}", clientIdentifier, session);
        return session;
    }

    @Override
    public Map<String, ServerSession> sessionMap() {
        return nodeBroker.sessionMap();
    }

    @Override
    public void destroySession(ServerSession session) {
        log.debug("ClusterBroker try to destroySession->{}", session);
        if (session instanceof ClusterServerSession) {
            ClusterServerSession css = (ClusterServerSession) session;
            // 清除 cluster leven Session
            clusterDbRepo.deleteSession(css);
            log.info("Session({}) was removed from the Cluster", session.clientIdentifier());
        } else if (session instanceof NodeServerSession) {
            nodeBroker.destroySession(session);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void connect(ServerSession session) {
        // Session 连接到 nodeBroker
        nodeBroker.connect(session);
        if (session instanceof ClusterServerSession) {
            ClusterServerSession css = (ClusterServerSession) session;
            // 1. 注册成功,绑定信息保存到 DB
            clusterDbRepo.saveSession(css.nodeId(nodeId()));
            // 2.从集群的订阅树中移除 Session 的离线订阅
            clusterDbRepo.removeOfflineSessionFromTopic(css.clientIdentifier(), css.subscriptions());
        }
        // publish Connect to Cluster
        publishConnectToCluster(session.clientIdentifier());
    }

    private void publishConnectToCluster(String clientIdentifier) {
        NodeMessage nm = NodeMessage.wrapConnect(nodeId(), clientIdentifier);
        Publish packet = Publish.outgoing(1, sessionChangePublishTopic(nodeId()), nm.toByteBuf());
        nodeBroker.forward(packet);
        log.debug("Client({}).Connect was published to the Cluster", clientIdentifier);
    }

    @Override
    public List<Subscribe.Subscription> subscribe(ServerSession session, Subscribe subscribe) {
        log.debug("Node({}) Session({}) receive Subscribe: {}", nodeId(), session.clientIdentifier(), subscribe);
        List<Subscribe.Subscription> subscriptions = nodeBroker.subscribe(session, subscribe);
        log.debug("Node({}) Session({}) permitted Subscribe: {}", nodeId(), session.clientIdentifier(), subscriptions);
        Set<String> tfSet = subscriptions.stream().map(Subscribe.Subscription::topicFilter).collect(toSet());
        clusterDbRepo.addNodeToTopicAsync(nodeId(), new ArrayList<>(tfSet))
                // 异步执行完成后若有异常直接关闭 session
                .exceptionally(e -> {
                    log.error("unExpected Exception", e);
                    session.close();
                    return null;
                });
        return subscriptions;
    }

    @Override
    public void unsubscribe(ServerSession session, Unsubscribe packet) {
        log.debug("Node({}) Session({}) receive Unsubscribe: {}", nodeId(), session.clientIdentifier(), packet);
        nodeBroker.unsubscribe(session, packet);
        // 清理结果不重要，即使清理失败，不影响流程
        removeNodeFromTopicAsync(session, new HashSet<>(packet.subscriptions()));
    }

    /**
     * 清理路由表
     * <p>异步清理</p>
     * <p>即使清理失败也可以接受，{@link ClusterBrokerImpl#forward(Publish)}中有兜底清理策略</p>
     */
    @Override
    public CompletableFuture<Void> removeNodeFromTopicAsync(ServerSession session, Set<Subscribe.Subscription> subscriptions) {
        List<String> topicToRemove = subscriptions.stream()
                .map(Subscribe.Subscription::topicFilter)
                .filter(topicFilter -> !nodeBroker.topic(topicFilter).isPresent())
                .collect(toList());
        return clusterDbRepo.removeNodeFromTopicAsync(nodeId(), topicToRemove);
    }

    @Override
    public Optional<Topic> topic(String topicFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Integer> supportProtocolLevel() {
        return nodeBroker.supportProtocolLevel();
    }

    @Override
    public List<Publish> retainMatch(String topicFilter) {
        return nodeBroker.retainMatch(topicFilter);
    }

    @Override
    public int forward(Publish packet) {
        // must set retain to false before forward the PublishPacket
        packet.retainFlag(false);
        int times = nodeBroker.forward(packet);
        // must retain the Publish.packet for async callback
        packet.retain();
        clusterDbRepo.matchTopicAsync(packet.topicName())
                .thenAccept((topics) -> forwardToBrokerAndOfflineSession(packet, topics))
                .whenComplete((v, t) -> {
                    // must release PublishPacket anyway
                    packet.release();
                    if (t != null) {
                        log.error("forward Publish failed-> Publish: {}", packet);
                        log.error("unExpected Exception", t);
                    }
                });
        return times;
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
            clusterDbRepo.removeNodeFromTopicAsync(targetNodeId, singletonList(ct.topicFilter()));
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
        ClusterServerSession s = clusterDbRepo.getSession(cId);
        if (s == null) {
            log.warn("forwardToOfflineSession failed, Session not exist -> {}", cId);
            return;
        }
        if (s.isOnline()) {
            log.warn("forwardToOfflineSession failed, Session is online -> {}", cId);
            return;
        }
        // use a shadow copy of the origin Publish
        Publish outgoing = Publish.outgoing(packet, tf, (byte) qos, packetIdentifier(s, qos));
        ClusterControlPacketContext cpx =
                new ClusterControlPacketContext(clusterDbRepo, cId, OUT, outgoing, INIT, null);
        clusterDbRepo.offerCpx(null, cpx);
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
        clusterDbRepo.close();
        // shutdown the nodeBroker
        nodeBroker.close();
        closeStatus.compareAndSet(1, 2);
        log.info("Broker is closed.");
    }

    private void closeAllSession() {
        for (Map.Entry<String, ServerSession> e : nodeBroker.sessionMap().entrySet()) {
            e.getValue().close();
            log.info("closeAllSession-> {}", e.getValue());
        }
    }

    @Override
    public Broker nodeBroker() {
        return nodeBroker;
    }

    @Override
    public ClusterDbRepo clusterDbRepo() {
        return clusterDbRepo;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"nodeId\":\"").append(nodeId()).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    @Timed(value = METRIC_NAME, histogram = true)
    public void handlePublish(Publish packet) {
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
            case INFO_CLUSTER_NODES:
                Set<NodeMessage.NodeInfo> state = m.unwrapClusterNodes();
                log.info("Broker receive Cluster.Nodes->{}", state);
                cluster.updateNodes(m.getNodeId(), state);
                break;
            case ACTION_BROKER_CLOSE:
                log.info("Broker receive shutdown Instruction");
                shutdownGracefully();
                break;
            case ACTION_SESSION_CLOSE:
                doHandleActionSessionClose(m);
                break;
            case ACTION_TOPIC_QUERY:
                doHandleActionTopicQuery(m);
                break;
            default:
                log.error("Broker receive Unknown Instruction->{}, {}", m, ByteBufUtil.prettyHexDump(packet.payload()));
        }
    }

    private void doHandleActionTopicQuery(NodeMessage m) {
        String topicFilter = m.unwrapSessionClose();
        Optional<Topic> topic = nodeBroker.topic(topicFilter);
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
        ServerSession session = nodeBroker.session(clientIdentifier);
        log.info("ClusterBroker receive Session.Closed-> cId: {}, session: {}", clientIdentifier, session);
        if (session != null) {
            session.close();
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
    public ServerSession createSession(Connect connect) {
        if (connect.cleanSession()) {
            log.debug("Broker now try create a new NodeServerSession");
            return NodeServerSession.from(connect);
        } else {
            log.debug("Broker now try create a new ClusterServerSession");
            return ClusterServerSession.from(connect);
        }
    }

    @Override
    public boolean closed() {
        return closeStatus.get() != 0;
    }

    @Override
    public boolean block(Publish packet) {
        return nodeBroker.block(packet);
    }

    @Override
    public void closeSession(ServerSession session) {
        // todo
    }

    @Override
    public void disconnectSessionFromNode(ClusterServerSession session) {
        session.nodeId(null);
        clusterDbRepo.saveSession(session);
        // important: 1 must be executed before 2
        // alter solution: 改变 addOfflineSessionToTopic 的实现，使其可以操作 订阅树
        // 1. Session离线，继续订阅主题
        clusterDbRepo.addOfflineSessionToTopic(session.clientIdentifier(), session.subscriptions());
        // 2.0 清除本 broker 中的 Session (even if CleanSession=0)
        nodeBroker().destroySession(session);
        // 2.1 清理路由表
        removeNodeFromTopicAsync(session, session.subscriptions());
    }

}
