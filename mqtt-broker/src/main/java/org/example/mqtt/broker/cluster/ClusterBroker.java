package org.example.mqtt.broker.cluster;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.model.Subscribe;

public interface ClusterBroker extends ClusterNode, Broker {

    Broker nodeBroker();

    ClusterBrokerState state();

}
