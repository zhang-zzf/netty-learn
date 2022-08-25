package org.example.mqtt.broker.metrics;

import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.DefaultServerSession;
import org.example.mqtt.broker.ServerSessionHandler;
import org.example.mqtt.model.Connect;

public class ServerSessionHandlerWithMetrics extends ServerSessionHandler {

    public ServerSessionHandlerWithMetrics(Broker broker, Authenticator authenticator, int activeIdleTimeoutSecond) {
        super(broker, authenticator, activeIdleTimeoutSecond);
    }

    @Override
    protected DefaultServerSession newServerSession(Connect connect) {
        return new DefaultServerSessionWithMetrics(connect);
    }

}
