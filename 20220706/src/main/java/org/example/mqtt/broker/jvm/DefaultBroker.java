package org.example.mqtt.broker.jvm;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.*;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@Slf4j
public class DefaultBroker implements Broker {

    private final BrokerState brokerState = new DefaultBrokerState();

    @Override
    public List<Subscribe.Subscription> subscribe(ServerSession session, Subscribe subscribe) {
        List<Subscribe.Subscription> subscriptions = subscribe.subscriptions();
        List<Subscribe.Subscription> permittedSub = new ArrayList<>(subscriptions.size());
        for (Subscribe.Subscription sub : subscriptions) {
            Subscribe.Subscription permitted = decideSubscriptionQos(session, sub);
            brokerState.subscribe(session, permitted);
            permittedSub.add(permitted);
        }
        return permittedSub;
    }

    @Override
    public void forward(Publish packet) {
        for (Topic topic : brokerState.match(packet.topicName())) {
            for (Map.Entry<ServerSession, Integer> e : topic.subscribers().entrySet()) {
                ServerSession session = e.getKey();
                if (!session.isRegistered()) {
                    continue;
                }
                String topicFilter = topic.topicFilter();
                int qos = Math.min(packet.qos(), e.getValue());
                // use a shadow copy of the origin Publish
                Publish outgoing = Publish.outgoing(packet, topicFilter, (byte) qos, session.nextPacketIdentifier());
                session.send(outgoing);
            }
        }
    }

    @Override
    public ServerSession session(String clientIdentifier) {
        return brokerState.session(clientIdentifier);
    }

    @Override
    public void disconnect(ServerSession session) {
        // remove the session from the broker
        brokerState.disconnect(session);
    }

    protected Subscribe.Subscription decideSubscriptionQos(ServerSession session, Subscribe.Subscription sub) {
        // todo decide qos
        int qos = sub.qos();
        return new Subscribe.Subscription(sub.topicFilter(), qos);
    }

    @Override
    public void deregister(ServerSession session, Unsubscribe packet) {
        for (Subscribe.Subscription subscription : packet.subscriptions()) {
            brokerState.unsubscribe(session, subscription);
        }
    }

    @Override
    public Set<Integer> supportProtocolLevel() {
        return new HashSet<>(Arrays.asList(4));
    }

    @SneakyThrows
    @Override
    public void connect(ServerSession session) {
        Future<ServerSession> future = brokerState.connect(session);
        ServerSession existSession = future.get();
        if (existSession != null && existSession.cleanSession()) {
            log.error("Session({}) cleanSession was not removed from broker.", existSession.clientIdentifier());
        }
    }

    @Override
    public void close() throws Exception {

    }

}
