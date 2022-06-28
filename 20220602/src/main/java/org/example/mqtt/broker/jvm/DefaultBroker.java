package org.example.mqtt.broker.jvm;

import io.netty.channel.Channel;
import org.example.mqtt.broker.*;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

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
    protected void bind(AbstractSession session) {
        AbstractSession previous = sessionMap.putIfAbsent(session.clientIdentifier(), session);
        if (previous != null) {
            throw new IllegalStateException();
        }
    }


    @Override
    public void onward(Publish packet) {

    }

    @Override
    public void disconnect(Session session) {
        // todo other clean job
        // remove the session from the broker
        sessionMap.remove(session.clientIdentifier(), session);
    }

    @Override
    protected Topic createNewTopic(Topic.TopicFilter topicFilter) {
        return new DefaultTopic(topicFilter);
    }

    @Override
    protected Topic findTopic(Topic.TopicFilter topicFilter) {
        return topicMap.get(topicFilter);
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
