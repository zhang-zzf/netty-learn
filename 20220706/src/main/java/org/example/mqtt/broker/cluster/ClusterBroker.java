package org.example.mqtt.broker.cluster;

import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.cluster.node.Cluster;

public interface ClusterBroker extends Broker {

    /**
     * Broker join the Cluster
     */
    void join(Cluster cluster);

    String nodeId();

    Broker nodeBroker();

    ClusterDbRepo clusterDbRepo();

    void disconnectSessionFromNode(ClusterServerSession session);

}
