package org.example.mqtt.broker.jvm;

import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Session;
import org.example.mqtt.broker.Topic;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
public class DefaultTopic implements Topic {

    private final String topicFilter;
    private final ConcurrentMap<ServerSession, Integer> subscribers;

    public DefaultTopic(String topicFilter) {
        this.topicFilter = topicFilter;
        this.subscribers = new ConcurrentHashMap<>();
    }

    @Override
    public String topicFilter() {
        return topicFilter;
    }

    @Override
    public Set<Session> retainedSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSubscriber(ServerSession session, int qos) {
        subscribers.put(session, qos);
    }

    @Override
    public void removeSubscriber(Session session) {
        subscribers.remove(session);
    }

    @Override
    public Map<ServerSession, Integer> subscribers() {
        return subscribers;
    }

    @Override
    public boolean isEmpty() {
        return subscribers.isEmpty();
    }

    @Override
    public void close() throws Exception {

    }

}
