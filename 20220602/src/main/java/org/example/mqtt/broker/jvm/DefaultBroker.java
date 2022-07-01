package org.example.mqtt.broker.jvm;

import org.example.mqtt.broker.*;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
public class DefaultBroker implements Broker {

    /**
     * ClientIdentifier -> Session
     */
    private ConcurrentMap<String, ServerSession> sessionMap = new ConcurrentHashMap<>();

    private ConcurrentMap<Topic.TopicFilter, Topic> topicMap = new ConcurrentHashMap<>();

    @Override
    public List<Subscription> register(List<Subscription> subscriptions) {
        List<Subscription> permittedSub = new ArrayList<>(subscriptions.size());
        for (Subscription sub : subscriptions) {
            Topic.TopicFilter topicFilter = new DefaultTopic.DefaultTopicFilter(sub.topicFilter());
            Topic topic = topicBy(topicFilter);
            int permittedQoS = decideSubscriptionQos(sub);
            topic.addSubscriber(sub.session(), permittedQoS);
            permittedSub.add(Subscription.from(sub.topicFilter(), permittedQoS, sub.session()));
        }
        return permittedSub;
    }

    @Override
    public void onward(Publish packet) {
        for (Map.Entry<Topic.TopicFilter, Topic> entry : topicMap.entrySet()) {
            if (!entry.getKey().match(packet.topicName())) {
                continue;
            }
            Topic topic = entry.getValue();
            for (Map.Entry<Session, Integer> subscriber : topic.subscribers().entrySet()) {
                Session session = subscriber.getKey();
                byte qos = subscriber.getValue().byteValue();
                /**
                 * retained payload will be released when it was remove from outQueue
                 */
                Publish outgoing = Publish.retained(packet).topicName(topic.topicFilter().value()).qos(qos);
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
        sessionMap.remove(session.clientIdentifier(), session);
    }

    protected Topic topicBy(Topic.TopicFilter topicFilter) {
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
            Topic.TopicFilter filter = new DefaultTopic.DefaultTopicFilter(sub.topicFilter());
            Topic topic = topicBy(filter);
            topic.removeSubscriber(sub.session());
            if (topic.isEmpty()) {
                // remove the topic from the broker
                topicMap.remove(filter, topic);
            }
        }
    }

    @Override
    public Set<Integer> supportProtocolLevel() {
        return new HashSet<>(Arrays.asList(4));
    }

    @Override
    public void bind(ServerSession session) {
        if (this.sessionMap.putIfAbsent(session.clientIdentifier(), session) != null) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void close() throws Exception {

    }

}
