package org.example.mqtt.broker.jvm;

import io.netty.channel.Channel;
import org.example.mqtt.broker.AbstractBroker;
import org.example.mqtt.broker.AbstractSession;
import org.example.mqtt.broker.Session;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscription;

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
    public void onward(Publish packet) {
        List<Topic> forwardTopic = topicBy(packet.getTopicName());
        for (Topic topic : forwardTopic) {
            String topicName = topic.topicFilter().value();
            for (Map.Entry<Session, Integer> entry : topic.subscribers().entrySet()) {
                Session session = entry.getKey();
                Publish message = Publish.outgoing(packet, topicName, entry.getValue());
                session.send(message);
            }
        }
    }

    @Override
    protected AbstractSession findSession(String clientIdentifier) {
        return sessionMap.get(clientIdentifier);
    }

    @Override
    protected boolean authenticate(Connect packet) {
        return true;
    }

    @Override
    protected AbstractSession createNewSession(Channel channel) {
        return new DefaultSession(channel, this);
    }

    @Override
    protected void bindSession(AbstractSession session) {
        AbstractSession previous = sessionMap.putIfAbsent(session.clientIdentifier(), session);
        if (previous != null) {
            throw new IllegalStateException();
        }
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
    protected int decideSubscriptionQos(Session session, Topic.TopicFilter topicFilter, int requiredQoS) {
        return requiredQoS;
    }

    @Override
    public void deregister(Session session, List<Subscription> subscriptions) {
        for (Subscription sub : subscriptions) {
            Topic.TopicFilter filter = new DefaultTopic.DefaultTopicFilter(sub.getTopic());
            Topic topic = topicBy(filter);
            topic.removeSubscriber(session);
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
