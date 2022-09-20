package org.example.mqtt.broker.cluster.node;

import java.util.UUID;

public class Node {

    public static final String NODE_ID_UNKNOWN = UUID.randomUUID().toString().replace("-", "");

    private final String id;
    /**
     * mqtt://host:port
     */
    private final String address;
    private volatile NodeClient nodeClient;

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

    public NodeClient nodeClient() {
        return nodeClient;
    }

    public Node nodeClient(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
        return this;
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
        if (nodeClient != null) {
            sb.append("\"nodeClient\":");
            String objectStr = nodeClient.toString().trim();
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

}
