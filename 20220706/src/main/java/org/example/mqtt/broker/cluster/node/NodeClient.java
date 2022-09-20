package org.example.mqtt.broker.cluster.node;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.client.Client;
import org.example.mqtt.client.MessageHandler;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.Arrays;
import java.util.Map;

import static org.example.mqtt.broker.cluster.node.Cluster.$_SYS_CLUSTER_NODES_TOPIC;
import static org.example.mqtt.broker.cluster.node.Cluster.$_SYS_NODES_TOPIC;
import static org.example.mqtt.broker.cluster.node.NodeMessage.*;

@Slf4j
public class NodeClient implements MessageHandler {

    private final ClusterBroker broker;
    private final Client client;
    private final Cluster cluster;

    public NodeClient(String address, Cluster cluster) {
        this.broker = cluster.broker();
        this.client = new Client(broker.nodeId(), address, this);
        this.cluster = cluster;
        initSubscribe();
    }

    private void initSubscribe() {
        Subscribe.Subscription nodeSubscription =
                new Subscribe.Subscription($_SYS_NODES_TOPIC + broker.nodeId(), Publish.EXACTLY_ONCE);
        Subscribe.Subscription clusterNodes =
                new Subscribe.Subscription($_SYS_CLUSTER_NODES_TOPIC, Publish.AT_LEAST_ONCE);
        this.client.subscribe(Arrays.asList(nodeSubscription, clusterNodes));
    }

    @Override
    public void handle(String topic, byte[] payload) {
        NodeMessage m = NodeMessage.fromBytes(payload);
        log.debug("NodeClient({}) receive message from {}: {}", cId(), m.getNodeId(), m);
        switch (m.getPacket()) {
            case PACKET_PUBLISH:
                // forward Publish
                Publish packet = m.unwrapPublish();
                log.debug("NodeClient({}) receive Publish: {}", cId(), packet);
                // just use LocalBroker to forward the PublishPacket
                broker.nodeBroker().forward(packet);
                break;
            case PACKET_CLUSTER_NODES:
                Map<String, String> state = m.unwrapClusterState();
                log.debug("NodeClient({}) receive Cluster State from {}: {}", cId(), m.getNodeId(), state);
                cluster.updateNodes(state);
                break;
            case SESSION_CLOSE:
                String clientIdentifier = m.unwrapSessionClose();
                log.debug("NodeClient({}) receive Session.Close from {}: {}", cId(), m.getNodeId(), clientIdentifier);
                ServerSession session = broker.session(clientIdentifier);
                if (session != null) {
                    session.close(false);
                    log.debug("NodeClient({}) Session({}).Closed.", cId(), clientIdentifier);
                } else {
                    log.warn("NodeClient({}) does not exist Session({})", cId(), clientIdentifier);
                }
            default:
                throw new IllegalArgumentException();
        }
    }

    private String cId() {
        return broker.nodeId();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
