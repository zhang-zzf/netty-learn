package org.example.mqtt.broker.cluster;

import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.model.Subscribe;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface ClusterBroker extends Broker {

    /**
     * Broker join the Cluster
     */
    void join(Cluster cluster);

    String nodeId();

    Broker nodeBroker();

    ClusterDbRepo clusterDbRepo();

    void disconnectSessionFromNode(ClusterServerSession session);

    CompletableFuture<Void> removeNodeFromTopicAsync(ServerSession session, Set<Subscribe.Subscription> subscriptions);

}
