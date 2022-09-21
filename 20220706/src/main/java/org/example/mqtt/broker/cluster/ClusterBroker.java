package org.example.mqtt.broker.cluster;

import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.cluster.node.NodeMessage;
import org.example.mqtt.broker.node.DefaultBroker;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;
import org.example.mqtt.session.ControlPacketContext;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

@Slf4j
public class ClusterBroker implements Broker {

    private final ClusterDbRepo clusterDbRepo;
    private final Broker nodeBroker = new DefaultBroker();

    /**
     * JVM 级别
     */
    private static final String nodeId = UUID.randomUUID().toString().replace("-", "");

    public ClusterBroker(ClusterDbRepo clusterDbRepo) {
        this.clusterDbRepo = clusterDbRepo;
    }

    @Override
    public ServerSession session(String clientIdentifier) {
        var localSession = nodeBroker.session(clientIdentifier);
        log.debug("Client({}) find session in LocalNode: {}", clientIdentifier, localSession);
        if (localSession != null) {
            log.debug("Client({}) find session in LocalNode: {}", clientIdentifier, localSession);
            return localSession;
        }
        var session = clusterDbRepo.getSessionByClientIdentifier(clientIdentifier);
        log.debug("Client({}) find session in Cluster: {}", clientIdentifier, session);
        return session;
    }

    @Override
    public void connect(ServerSession session) {
        nodeBroker.connect(session);
    }

    @Override
    public void disconnect(ServerSession session) {
        nodeBroker.disconnect(session);
    }

    @Override
    public List<Subscribe.Subscription> subscribe(ServerSession session, Subscribe subscribe) {
        log.debug("Node({}) Session({}) receive Subscribe: {}", nodeId(), session.clientIdentifier(), subscribe);
        List<Subscribe.Subscription> subscriptions = nodeBroker.subscribe(session, subscribe);
        log.debug("Node({}) Session({}) permitted Subscribe: {}", nodeId(), session.clientIdentifier(), subscriptions);
        Set<String> tfSet = subscriptions.stream().map(Subscribe.Subscription::topicFilter).collect(toSet());
        // super.subscribe 成功，但是 cluster 操作失败（抛出异常）。-> Session.close()
        clusterDbRepo.addNodeToTopic(nodeId(), new ArrayList<>(tfSet));
        return subscriptions;
    }

    @Override
    public void unsubscribe(ServerSession session, Unsubscribe packet) {
        log.debug("Node({}) Session({}) receive Unsubscribe: {}", nodeId(), session.clientIdentifier(), packet);
        nodeBroker.unsubscribe(session, packet);
        List<String> topicToRemove = packet.subscriptions().stream()
                .map(s -> s.topicFilter())
                .filter(topicFilter -> !nodeBroker.topic(topicFilter).isPresent())
                .collect(toList());
        clusterDbRepo.removeNodeFromTopic(nodeId(), topicToRemove);
        log.debug("Node({}) Session({}) unsubscribe done", nodeId(), session.clientIdentifier());
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
    public void retain(Publish packet) {
        nodeBroker.retain(packet);
    }

    @Override
    public List<Publish> retainMatch(String topicFilter) {
        return nodeBroker.retainMatch(topicFilter);
    }

    @Override
    public void forward(Publish packet) {
        // must set retain to false before forward the PublishPacket
        packet.retain(false);
        // Node local forward
        nodeBroker.forward(packet);
        // forward to offline Clients
        List<ClusterTopic> clusterTopics = clusterDbRepo.matchTopic(packet.topicName());
        log.debug("Publish({}) match Cluster Topics: {}", packet.topicName(), clusterTopics);
        for (ClusterTopic ct : clusterTopics) {
            // forward to another Node in the cluster
            for (String targetNodeId : ct.getNodes()) {
                if (nodeId().equals(targetNodeId)) {
                    continue;
                }
                forwardToOtherNode(packet, targetNodeId);
            }
            // forward the PublishPacket to offline client's Session.
            for (Map.Entry<String, Byte> e : ct.getOfflineSessions().entrySet()) {
                forwardToOfflineSession(packet, ct, e);
            }
        }
    }

    private void forwardToOtherNode(Publish packet, String targetNodeId) {
        NodeMessage nm = NodeMessage.wrapPublish(nodeId(), packet);
        String topicName = Cluster.$_SYS_NODES_TOPIC + targetNodeId;
        Publish outgoing = Publish.outgoing(packet.qos(), topicName, nm.toByteBuf());
        log.debug("Publish forward to other Node: {}", nodeId());
        nodeBroker.forward(outgoing);
    }

    // todo
    private void forwardToOfflineSession(Publish packet, ClusterTopic ct, Map.Entry<String, Byte> e) {
        String clientIdentifier = e.getKey();
        ClusterServerSession s = clusterDbRepo.getSessionByClientIdentifier(clientIdentifier);
        if (s == null) {
            log.warn("Publish forward to offline Client[Session 不存在]: {}", clientIdentifier);
            return;
        }
        // use a shadow copy of the origin Publish
        Publish outgoing = Publish.outgoing(packet, ct.topicFilter(), e.getValue(), s.nextPacketIdentifier());
        ControlPacketContext cpx = s.createNewCpx(outgoing, INIT, OUT);
        boolean added = clusterDbRepo.offerToOutQueueOfTheOfflineSession(s, (ClusterControlPacketContext) cpx);
        if (!added) {
            log.warn("Publish forward to offline Client[添加失败]: {}", clientIdentifier);
            ClusterServerSession curS = clusterDbRepo.getSessionByClientIdentifier(clientIdentifier);
            if (curS.isBound()) {
                log.warn("Pubish forward to offline Client[添加失败，Client online], now forward it: {}", clientIdentifier);
                // todo online forward
                String nodeId = curS.nodeId();
            }
        }
    }

    public String nodeId() {
        return nodeId;
    }

    @Override
    public void close() throws Exception {
        nodeBroker.close();
    }

    public Broker nodeBroker() {
        return nodeBroker;
    }

    public ClusterDbRepo clusterDbRepo() {
        return clusterDbRepo;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"nodeId\":\"").append(nodeId()).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
