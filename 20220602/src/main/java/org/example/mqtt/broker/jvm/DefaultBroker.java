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
public class DefaultBroker extends AbstractBroker {

    /**
     * ClientIdentifier -> Session
     */
    private ConcurrentMap<String, AbstractSession> sessionMap = new ConcurrentHashMap<>();

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
        List<Topic> forwardTopic = topicBy(packet.topicName());
        for (Topic topic : forwardTopic) {
            String topicName = topic.topicFilter().value();
            for (Map.Entry<Session, Integer> entry : topic.subscribers().entrySet()) {
                Session session = entry.getKey();
                byte qos = entry.getValue().byteValue();
                /**
                 * retained payload will be released when it was remove from outQueue
                 */
                session.send(Publish.retained(packet).topicName(topicName).qos(qos));
            }
        }
    }

    @Override
    public Session accepted(Connect packet) throws Exception {
        AbstractSession session = sessionMap.get(packet.clientIdentifier());
        // clean session is set to 0
        if (!packet.cleanSession() && session != null) {
            return session;
        }
        // clean session is set to 1
        if (packet.cleanSession() && session != null) {
            // discard any previous session if exist
            session.close();
        }
        // create and init session
        session = initAndBind(packet);
        return session;
    }


    @Override
    protected AbstractSession initAndBind(Connect packet) {
        // AbstractSession session = new AbstractSession(this);
        AbstractSession session = null;
        session.clientIdentifier(packet.clientIdentifier());
        session.cleanSession(packet.cleanSession());
        // session bind to broker;
        if (sessionMap.putIfAbsent(session.clientIdentifier(), session) != null) {
            // concurrent create the same clientIdentifier session
            throw new IllegalStateException();
        }
        return session;
    }

    @Override
    public void disconnect(Session session) {
        // todo other clean job
        // remove the session from the broker
        sessionMap.remove(session.clientIdentifier(), session);
    }

    @Override
    protected List<Topic> topicBy(String topicName) {
        // todo should optimize match algorithm
        List<Topic> ret = new ArrayList<>();
        for (Map.Entry<Topic.TopicFilter, Topic> entry : topicMap.entrySet()) {
            if (entry.getKey().match(topicName)) {
                ret.add(entry.getValue());
            }
        }
        return ret;
    }

    @Override
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

    @Override
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
    public void close() throws Exception {

    }

}
