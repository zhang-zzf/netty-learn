package org.example.mqtt.broker.metrics;

import org.example.mqtt.broker.*;
import org.example.mqtt.model.Connect;

public class ServerSessionHandlerWithMetrics extends ServerSessionHandler {

    public ServerSessionHandlerWithMetrics(Broker broker, Authenticator authenticator, int activeIdleTimeoutSecond) {
        super(broker, authenticator, activeIdleTimeoutSecond);
    }

    @Override
    protected ServerSession buildServerSession(ServerSession preSession, Connect connect) {
        if (preSession == null) {
            return new DefaultServerSessionWithMetrics(connect);
        } else {
            return ((DefaultServerSessionWithMetrics) preSession).init(connect);
        }
    }

}
