package org.example.mqtt.broker.cluster.node;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.client.Client;
import org.example.mqtt.client.MessageHandler;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.example.mqtt.broker.cluster.node.Cluster.*;
import static org.example.mqtt.broker.cluster.node.NodeMessage.*;

@Slf4j
public class NodeClient implements MessageHandler, AutoCloseable {

    private final Client client;
    private final Cluster cluster;
    private final Node node;
    private final static AtomicLong clientIdentifierCounter = new AtomicLong(0);

    private static final int CPU_NUM = Runtime.getRuntime().availableProcessors();

    private ExecutorService executorService = new ThreadPoolExecutor(
            CPU_NUM, CPU_NUM * 2, 60, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(CPU_NUM),
            new DefaultThreadFactory(NodeClient.class.getSimpleName(), false),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    public NodeClient(Node node, Cluster cluster) {
        this.node = node;
        this.cluster = cluster;
        String clientIdentifier = cluster.broker().nodeId() + "/" + clientIdentifierCounter.getAndIncrement();
        this.client = new Client(clientIdentifier, node.address(), this);
        initSubscribe();
    }

    private void initSubscribe() {
        String subscribeOnlyMyself = nodeListenTopic(broker().nodeId());
        Subscribe.Subscription nodeSubscription =
                new Subscribe.Subscription(subscribeOnlyMyself, Publish.EXACTLY_ONCE);
        Subscribe.Subscription clusterNodes =
                new Subscribe.Subscription(subscribeAllNode(), Publish.AT_LEAST_ONCE);
        List<Subscribe.Subscription> sub = Arrays.asList(nodeSubscription, clusterNodes);
        log.debug("NodeClient try to subscribe: {}", sub);
        client.syncSubscribe(sub);
    }

    private ClusterBroker broker() {
        return cluster.broker();
    }

    @Override
    public void handle(String topic, byte[] payload, Publish packet) {
        NodeMessage m = NodeMessage.fromBytes(payload);
        log.debug("NodeClient receive message->{}", m);
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
                doAsyncHandleActionPublishForward(m);
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
                throw new IllegalArgumentException();
        }
    }

    private void doHandleActionSessionClose(NodeMessage m) {
        String clientIdentifier = m.unwrapSessionClose();
        ServerSession session = broker().session(clientIdentifier);
        log.info("NodeClient receive Session.Closed. cur Session->{}", session);
        if (session != null) {
            session.close();
            log.info("NodeClient Session.Closed->{}", clientIdentifier);
        } else {
            log.warn("NodeClient does not exist Session({})", clientIdentifier);
        }
    }

    private void doHandleInfoClusterNodes(NodeMessage m) {
        Map<String, String> state = m.unwrapClusterNodes();
        log.debug("NodeClient receive Cluster.Nodes: {}", state);
        cluster.updateNodes(state);
    }

    private void doAsyncHandleActionPublishForward(NodeMessage m) {
        // forward Publish
        executorService.submit(() -> doHandleActionPublishForward(m));
    }

    private void doHandleActionPublishForward(NodeMessage m) {
        // forward Publish
        Publish packet = m.unwrapPublish();
        log.debug("NodeClient receive Publish: {}", packet);
        // just use LocalBroker to forward the PublishPacket
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
        NodeMessage nm = wrapClusterNodes(broker().nodeId(), nodes);
        client.syncSend(Publish.AT_LEAST_ONCE, $_SYS_TOPIC, nm.toByteBuf());
    }

    @Override
    public void close() throws Exception {
        log.info("NodeClient({}) now try to close", this);
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        }
        client.close();
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

}
