package org.example.mqtt.broker.cluster.node;

import lombok.Getter;

public class Node {

    public static final String NODE_ID_UNKNOWN = "NODE_ID_UNKNOWN";

    @Getter
    private final String id;
    /**
     * mqtt://host:port
     */
    @Getter
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
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
