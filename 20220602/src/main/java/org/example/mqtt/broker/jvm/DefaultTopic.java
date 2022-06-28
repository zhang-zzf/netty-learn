package org.example.mqtt.broker.jvm;

import org.example.mqtt.broker.Session;
import org.example.mqtt.broker.Topic;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
public class DefaultTopic implements Topic {

    private final TopicFilter topicFilter;
    private final ConcurrentMap<Session, Integer> subscriber;

    public DefaultTopic(TopicFilter topicFilter) {
        this.topicFilter = topicFilter;
        this.subscriber = new ConcurrentHashMap<>();
    }

    @Override
    public TopicFilter topic() {
        return topicFilter;
    }

    @Override
    public Set<Session> retainedSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSubscriber(Session session, int qos) {
        subscriber.put(session, qos);
    }

    @Override
    public Map<Session, Integer> subscribers() {
        return null;
    }

    public static class DefaultTopicFilter implements TopicFilter {

        private final String value;

        public DefaultTopicFilter(String value) {
            this.value = value;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public boolean match(String topicName) {
            return value.equals(topicName);
        }

    }

}
