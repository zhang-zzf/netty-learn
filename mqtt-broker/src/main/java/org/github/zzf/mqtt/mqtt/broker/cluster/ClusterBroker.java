package org.github.zzf.mqtt.mqtt.broker.cluster;

import org.github.zzf.mqtt.protocol.server.Broker;

public interface ClusterBroker extends  Broker {

    String nodeId();

    Broker nodeBroker();

    ClusterBrokerState state();

}
