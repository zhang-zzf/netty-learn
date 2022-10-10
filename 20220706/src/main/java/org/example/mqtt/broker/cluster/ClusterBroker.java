package org.example.mqtt.broker.cluster;

import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.model.Subscribe;

import java.util.Set;

public interface ClusterBroker extends Broker {

    /**
     * Broker join the Cluster
     */
    void join(Cluster cluster);

    String nodeId();

    Broker nodeBroker();

    ClusterDbRepo clusterDbRepo();

    void disconnectSessionFromNode(ClusterServerSession session);

    void removeNodeFromTopic(Set<Subscribe.Subscription> subscriptions);

}
