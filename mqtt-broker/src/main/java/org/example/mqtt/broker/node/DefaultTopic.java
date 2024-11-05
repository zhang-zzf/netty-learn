package org.example.mqtt.broker.node;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/06/28
 */
@Slf4j
public class DefaultTopic implements Topic {

    private final String topicFilter;
    private final ConcurrentMap<ServerSession, Integer> subscribers = new ConcurrentHashMap<>(1);

    public DefaultTopic(String topicFilter) {
        this.topicFilter = topicFilter;
    }

    @Override
    public String topicFilter() {
        return topicFilter;
    }

    @Override
    public Map<ServerSession, Integer> subscribers() {
        return Collections.unmodifiableMap(subscribers);
    }

    @Override
    public void subscribe(ServerSession session, int qos) {
        subscribers.put(session, qos);
    }

    @Override
    public void unsubscribe(ServerSession session, int qos) {
        subscribers.remove(session, qos);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (topicFilter != null) {
            sb.append("\"topicFilter\":\"").append(topicFilter).append('\"').append(',');
        }
        if (subscribers != null) {
            sb.append("\"subscribers\":");
            if (!(subscribers).isEmpty()) {
                sb.append("{");
                final Set<?> mapKeySet = (subscribers).keySet();
                for (Object mapKey : mapKeySet) {
                    final Object mapValue = (subscribers).get(mapKey);
                    sb.append("\"").append(mapKey).append("\":\"").append(Objects.toString(mapValue, "")).append("\",");
                }
                sb.replace(sb.length() - 1, sb.length(), "}");
            } else {
                sb.append("{}");
            }
            sb.append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
