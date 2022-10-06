package org.example.mqtt.broker.cluster;

import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.node.DefaultBroker;

public interface ClusterBroker extends Broker {

    /**
     * Broker join the Cluster
     */
    void join(Cluster cluster);

    String nodeId();

    DefaultBroker nodeBroker();

    ClusterDbRepo clusterDbRepo();

    void disconnectFromNodeBroker(ClusterServerSession session);

}
