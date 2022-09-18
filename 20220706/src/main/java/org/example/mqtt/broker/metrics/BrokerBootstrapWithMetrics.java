package org.example.mqtt.broker.metrics;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.node.DefaultBroker;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;

import javax.net.ssl.SSLException;
import java.net.URISyntaxException;
import java.util.function.Supplier;

import static org.example.mqtt.broker.node.bootstrap.BrokerBootstrap.startServer;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrapWithMetrics {

    public static void main(String[] args) throws URISyntaxException, SSLException {
        Authenticator authenticator = packet -> 0x00;
        final Broker broker = new DefaultBroker();
        Supplier<DefaultServerSessionHandler> handlerSupplier = () ->
                new ServerSessionHandlerWithMetrics(broker, authenticator, 3);
        startServer(handlerSupplier);
    }

}
