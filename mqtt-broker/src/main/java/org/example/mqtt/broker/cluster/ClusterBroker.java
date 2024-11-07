package org.example.mqtt.broker.cluster;

import org.example.mqtt.broker.Broker;

public interface ClusterBroker extends  Broker {

    String nodeId();

    Broker nodeBroker();

    ClusterBrokerState state();

}
