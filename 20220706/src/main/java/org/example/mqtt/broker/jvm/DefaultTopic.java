package org.example.mqtt.broker.jvm;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.session.Session;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@Slf4j
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
        Integer removed = subscribers.remove(session);
        if (removed != null) {
            log.info("Session({}) unsubscribe from Topic({}) success", session.clientIdentifier(), this);
        } else {
            log.error("Session({}) unsubscribe from Topic({}) failed", session.clientIdentifier(), this);
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultTopic that = (DefaultTopic) o;
        return topicFilter.equals(that.topicFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicFilter);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (topicFilter != null) {
            sb.append("\"topicFilter\":\"").append(topicFilter).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
