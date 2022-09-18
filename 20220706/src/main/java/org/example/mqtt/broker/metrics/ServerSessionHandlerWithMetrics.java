package org.example.mqtt.broker.metrics;

import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;
import org.example.mqtt.model.Connect;

public class ServerSessionHandlerWithMetrics extends DefaultServerSessionHandler {

    public ServerSessionHandlerWithMetrics(Broker broker, Authenticator authenticator, int activeIdleTimeoutSecond) {
        super(broker, authenticator, activeIdleTimeoutSecond);
    }

    @Override
    protected ServerSession doCreateNewSession(Connect connect) {
        return DefaultServerSessionWithMetrics.from(connect);
    }

}
