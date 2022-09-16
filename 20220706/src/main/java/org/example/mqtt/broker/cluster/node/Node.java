package org.example.mqtt.broker.cluster.node;

public class Node {

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

    public void nodeClient(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }


}
