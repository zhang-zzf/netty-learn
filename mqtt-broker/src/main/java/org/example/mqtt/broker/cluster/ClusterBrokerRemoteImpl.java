package org.example.mqtt.broker.cluster;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2024-11-06
 */
public class ClusterBrokerRemoteImpl implements ClusterNode {

    private final String nodeId;

    public ClusterBrokerRemoteImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

}