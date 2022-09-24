package org.example.mqtt.broker.cluster.node;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.client.Client;
import org.example.mqtt.client.MessageHandler;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.example.mqtt.broker.cluster.node.Cluster.*;
import static org.example.mqtt.broker.cluster.node.NodeMessage.*;

@Slf4j
public class NodeClient implements MessageHandler, AutoCloseable {

    private final Client client;
    private final Cluster cluster;
    private final Node node;

    public NodeClient(Node node, Cluster cluster) {
        this.node = node;
        this.cluster = cluster;
        String clientIdentifier = cluster.broker().nodeId();
        this.client = new Client(clientIdentifier, node.address(), this);
        initSubscribe();
    }

    private void initSubscribe() {
        String subscribeOnlyMyself = nodeListenTopic(broker().nodeId());
        Subscribe.Subscription nodeSubscription =
                new Subscribe.Subscription(subscribeOnlyMyself, Publish.EXACTLY_ONCE);
        Subscribe.Subscription clusterNodes =
                new Subscribe.Subscription(subscribeAllNode(), Publish.AT_LEAST_ONCE);
        client.subscribe(Arrays.asList(nodeSubscription, clusterNodes));
    }

    private ClusterBroker broker() {
        return cluster.broker();
    }

    @Override
    public void handle(String topic, byte[] payload) {
        NodeMessage m = NodeMessage.fromBytes(payload);
        log.debug("NodeClient receive message->{}", m);
        switch (m.getPacket()) {
            case ACTION_PUBLISH_FORWARD:
                // forward Publish
                Publish packet = m.unwrapPublish();
                log.debug("NodeClient receive Publish: {}", packet);
                // just use LocalBroker to forward the PublishPacket
                broker().nodeBroker().forward(packet);
                break;
            case INFO_CLUSTER_NODES:
                Map<String, String> state = m.unwrapClusterState();
                cluster.updateNodes(state);
                break;
            case ACTION_SESSION_CLOSE:
                String clientIdentifier = m.unwrapSessionClose();
                ServerSession session = broker().session(clientIdentifier);
                if (session != null) {
                    session.close();
                    log.debug("NodeClient Session({}).Closed.", clientIdentifier);
                } else {
                    log.warn("NodeClient does not exist Session({})", clientIdentifier);
                }
                break;
            case INFO_CLIENT_CONNECT:
                doHandleClientConnect(m);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void doHandleClientConnect(NodeMessage m) {
        String clientIdentifier = m.unwrapSessionClose();
        // just get Session from local Node(Broker)
        ServerSession session = broker().nodeBroker().session(clientIdentifier);
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
        cluster.removeNode(node);
    }

    private String cId() {
        return broker().nodeId();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (node != null) {
            sb.append("\"node\":");
            String objectStr = node.toString().trim();
            if (objectStr.startsWith("{") && objectStr.endsWith("}")) {
                sb.append(objectStr);
            } else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            } else {
                sb.append("\"").append(objectStr).append("\"");
            }
            sb.append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public void syncLocalNode(Map<String, String> nodes) throws ExecutionException, InterruptedException {
        NodeMessage nm = wrapClusterState(broker().nodeId(), nodes);
        client.syncSend(Publish.AT_LEAST_ONCE, $_SYS_TOPIC, nm.toByteBuf());
    }

    @Override
    public void close() throws Exception {
        log.info("NodeClient({}) now try to close", this);
        client.close();
    }

}
