package org.github.zzf.mqtt.mqtt.broker.node;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.session.server.ServerSession;
import org.github.zzf.mqtt.protocol.session.server.Topic;

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
    public List<Subscriber> subscribers() {
        // todo
        throw new UnsupportedOperationException();
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
