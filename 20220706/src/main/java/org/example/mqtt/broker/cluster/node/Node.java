package org.example.mqtt.broker.cluster.node;

import lombok.Getter;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.stream.Collectors.toSet;

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
        this.nodeClients.add(nodeClient);
        return this;
    }

    public boolean removeNodeClient(NodeClient nc) {
        for (int i = 0; i < nodeClients.size(); i++) {
            if (nodeClients.get(i).getClientIdentifier().equals(nc.getClientIdentifier())) {
                nodeClients.remove(i);
                return true;
            }
        }
        return false;
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
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public Set<String> nodeClientIdSet() {
        return nodeClients.stream()
                .map(NodeClient::getClientIdentifier)
                .collect(toSet());
    }

}
