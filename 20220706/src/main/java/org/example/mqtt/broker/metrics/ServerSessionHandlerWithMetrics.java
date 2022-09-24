package org.example.mqtt.broker.metrics;

import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;

public class ServerSessionHandlerWithMetrics extends DefaultServerSessionHandler {

    public ServerSessionHandlerWithMetrics(Broker broker, Authenticator authenticator, int activeIdleTimeoutSecond) {
        super(broker, authenticator, activeIdleTimeoutSecond);
    }

}
