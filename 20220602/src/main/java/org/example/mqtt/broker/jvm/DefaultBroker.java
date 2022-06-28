package org.example.mqtt.broker.jvm;

import io.netty.channel.Channel;
import org.example.mqtt.broker.AbstractBroker;
import org.example.mqtt.broker.AbstractSession;
import org.example.mqtt.broker.Session;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Subscribe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    public void deregister(Session session, Subscribe subscribe) {

    }

    @Override
    public void close() throws Exception {

    }

}
