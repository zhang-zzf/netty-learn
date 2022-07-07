package org.example.mqtt.broker.jvm;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Subscription;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Publish;
import org.example.mqtt.session.Session;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@Slf4j
public class DefaultBroker implements Broker {

    /**
     * ClientIdentifier -> Session
     */
    private final ConcurrentMap<String, ServerSession> sessionMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Topic> topicMap = new ConcurrentHashMap<>();

    @Override
    public List<Subscription> register(List<Subscription> subscriptions) {
        List<Subscription> permittedSub = new ArrayList<>(subscriptions.size());
        for (Subscription sub : subscriptions) {
            Topic topic = topicBy(sub.topicFilter());
            int permittedQoS = decideSubscriptionQos(sub);
            topic.addSubscriber(sub.session(), permittedQoS);
            permittedSub.add(Subscription.from(sub.topicFilter(), permittedQoS, sub.session()));
        }
        return permittedSub;
    }

    @Override
    public void onward(Publish packet) {
        for (Map.Entry<String, Topic> entry : topicMap.entrySet()) {
            // todo use matcher
            if (!entry.getKey().equals(packet.topicName())) {
                continue;
            }
            Topic topic = entry.getValue();
            for (Map.Entry<ServerSession, Integer> subscriber : topic.subscribers().entrySet()) {
                ServerSession session = subscriber.getKey();
                if (!session.isRegistered()) {
                    log.error("Session deregister from the broker, but there is a subscribe linked to the session." +
                            "{}", session.clientIdentifier());
                    continue;
                }
                byte qos = subscriber.getValue().byteValue();
                // use a shadow copy of the origin Publish
                Publish outgoing = Publish.outgoing(packet, false, topic.topicFilter(), qos,
                        session.nextPacketIdentifier());
                session.send(outgoing);
            }
        }
    }

    @Override
    public ServerSession session(String clientIdentifier) {
        return sessionMap.get(clientIdentifier);
    }

    @Override
    public void disconnect(Session session) {
        // todo other clean job
        // remove the session from the broker
        boolean removed = sessionMap.remove(session.clientIdentifier(), session);
        if (!removed) {
            log.error("Session({}) disconnect failed.", session.clientIdentifier());
        } else {
            log.info("Session({}) disconnect success.", session.clientIdentifier());
        }
        // clean all the subscription that the session registered
        for (Subscription sub : session.subscriptions()) {
            Topic topic = topicMap.get(sub.topicFilter());
            if (topic != null) {
                topic.removeSubscriber(sub.session());
                if (topic.isEmpty()) {
                    boolean topicRemoved = topicMap.remove(sub.topicFilter(), topic);
                    if (topicRemoved) {
                        log.info("Topic({}) has none Subscriber, and broker remove it.", topic);
                    } else{
                        log.error("Topic({}) has none Subscriber, but broker remove it failed.", topic);
                    }
                }
            }
        }
    }

    protected Topic topicBy(String topicFilter) {
        Topic topic = topicMap.get(topicFilter);
        if (topic == null) {
            // create the topic if not Exist
            topic = new DefaultTopic(topicFilter);
            if (topicMap.putIfAbsent(topicFilter, topic) != null) {
                topic = topicMap.get(topicFilter);
            }
        }
        return topic;
    }

    protected int decideSubscriptionQos(Subscription sub) {
        return sub.qos();
    }

    @Override
    public void deregister(List<Subscription> subscriptions) {
        for (Subscription sub : subscriptions) {
            String topicFilter = sub.topicFilter();
            Topic topic = topicBy(topicFilter);
            topic.removeSubscriber(sub.session());
            if (topic.isEmpty()) {
                // remove the topic from the broker
                topicMap.remove(topicFilter, topic);
            }
        }
    }

    @Override
    public Set<Integer> supportProtocolLevel() {
        return new HashSet<>(Arrays.asList(4));
    }

    @Override
    public void connect(ServerSession session) {
        ServerSession existSession = sessionMap.putIfAbsent(session.clientIdentifier(), session);
        if (existSession.cleanSession()) {
            log.error("Session({}) cleanSession was not removed from broker.", existSession.clientIdentifier());
        }
    }

    @Override
    public void close() throws Exception {

    }

}
