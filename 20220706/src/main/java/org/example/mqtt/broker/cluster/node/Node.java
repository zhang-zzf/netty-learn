package org.example.mqtt.broker.cluster.node;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;

@Slf4j
public class Node implements AutoCloseable {

    public static final String NODE_ID_UNKNOWN = "NODE_ID_UNKNOWN";

    @Getter
    private String id;
    /**
     * mqtt://host:port
     */
    @Getter
    private final String address;

    // thread safe
    private final CopyOnWriteArrayList<NodeClient> nodeClients = new CopyOnWriteArrayList<>();
    /**
     * 集群广播消息专用 client
     * <p>nodeClients 中的其中一个 Client 肩负订阅集群消息</p>
     */
    private final AtomicReference<NodeClient> clusterMessageClient = new AtomicReference();

    /**
     * 连接失败次数统计
     */
    private final AtomicInteger connectFailCnt = new AtomicInteger();


    private final AtomicBoolean updating = new AtomicBoolean(false);

    public Node(String id, String address) {
        this.id = id;
        this.address = address;
    }

    public String id() {
        return id;
    }

    public String address() {
        return address;
    }

    public int nodeClientsCnt() {
        return nodeClients.size();
    }

    /**
     * 随机挑选一个 NodeClient
     * <p>当且仅当 NodeClient 不存在时，返回 null</p>
     */
    @Nullable
    public NodeClient nodeClient() {
        if (nodeClients.isEmpty()) {
            return null;
        }
        return nodeClients.get(ThreadLocalRandom.current().nextInt(nodeClients.size()));
    }

    public Node addNodeClient(NodeClient nodeClient) {
        nodeClients.add(nodeClient);
        trySubscribeClusterMessage(nodeClient);
        // 添加新节点， Node 是可达的
        connectFailCnt.set(0);
        return this;
    }

    public boolean removeNodeClient(NodeClient nc) {
        log.debug("removeNodeClient-> NodeClient: {}", nc);
        boolean removed = false;
        boolean clusterMessageClientRemoved = false;
        for (int i = 0; i < nodeClients.size(); i++) {
            if (nodeClients.get(i).getClientIdentifier().equals(nc.getClientIdentifier())) {
                nodeClients.remove(i);
                log.debug("removeNodeClient removed-> NodeClient: {}", nc);
                removed = true;
                if (clusterMessageClient.get() == nc) {
                    log.debug("removeNodeClient clusterMessageClient removed-> NodeClient: {}", nc);
                    // 移除的是订阅集群广播消息的客户端
                    clusterMessageClient.set(null);
                    clusterMessageClientRemoved = true;
                }
                break;
            }
        }
        if (clusterMessageClientRemoved && !nodeClients.isEmpty()) {
            log.debug("removeNodeClient trySubscribeClusterMessage");
            trySubscribeClusterMessage(nodeClients.get(0));
        }
        return removed;
    }

    private void trySubscribeClusterMessage(NodeClient nodeClient) {
        log.debug("trySubscribeClusterMessage-> cur: {}", clusterMessageClient.get());
        if (clusterMessageClient.compareAndSet(null, nodeClient)) {
            log.debug("trySubscribeClusterMessage new NodeClient-> NodeClient: {}", nodeClient);
            boolean subscribed = nodeClient.subscribeClusterMessage();
            if (!subscribed) {
                log.error("trySubscribeClusterMessage failed-> NodeClient: {}", nodeClient);
                clusterMessageClient.compareAndSet(nodeClient, null);
            }
        }
    }

    public Node nodeId(String nodeId) {
        this.id = nodeId;
        return this;
    }

    @Override
    public void close() throws Exception {
        for (NodeClient client : nodeClients) {
            client.close();
        }
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (id != null) {
            sb.append("\"id\":\"").append(id).append('\"').append(',');
        }
        if (address != null) {
            sb.append("\"address\":\"").append(address).append('\"').append(',');
        }
        if (nodeClients != null) {
            sb.append("\"nodeClients\":");
            if (!(nodeClients).isEmpty()) {
                sb.append("[");
                final int listSize = (nodeClients).size();
                for (int i = 0; i < listSize; i++) {
                    final Object listValue = (nodeClients).get(i);
                    if (listValue instanceof CharSequence) {
                        sb.append("\"").append(Objects.toString(listValue, "")).append("\"");
                    } else {
                        sb.append(Objects.toString(listValue, ""));
                    }
                    if (i < listSize - 1) {
                        sb.append(",");
                    } else {
                        sb.append("]");
                    }
                }
            } else {
                sb.append("[]");
            }
            sb.append(',');
        }
        if (clusterMessageClient.get() != null) {
            sb.append("\"clusterMessageClient\":").append(clusterMessageClient.get()).append(",");
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public Set<String> nodeClientIdSet() {
        return nodeClients.stream()
                .map(NodeClient::getClientIdentifier)
                .collect(toSet());
    }

    public int connectFailed() {
        return connectFailCnt.incrementAndGet();
    }

    public boolean markUpdating() {
        return updating.compareAndSet(false, true);
    }

    public void clearUpdating() {
        updating.set(false);
    }

    public NodeClient cmClient() {
        return ofNullable(clusterMessageClient.get())
                .orElse(null);
    }

    public void checkNodeClientAvailable() {
        for (NodeClient nc : nodeClients) {
           // todo
        }
    }

}
