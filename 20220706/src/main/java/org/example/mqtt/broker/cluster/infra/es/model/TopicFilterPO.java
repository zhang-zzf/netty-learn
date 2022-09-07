package org.example.mqtt.broker.cluster.infra.es.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 集群级别 TopicFilter 模型
 * <p> "topic_abc_de" use as ID </p>
 */
@Data
public class TopicFilterPO {

    /**
     * topic/abc/de
     */
    private String value;
    /**
     * "1" -> "topic"
     * "2" -> "abc"
     * "3" -> "de"
     */
    private final Map<String, String> topicLevel = new HashMap<>(8);
    /**
     * 订阅此 TopicFilter 的 node 节点
     */
    private Set<String> subscribeNodes;
    /**
     * 订阅此 TopicFilter 的离线 Session
     */
    private Set<SubscriptionPO> offlineSessions;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (value != null) {
            sb.append("\"id\":\"").append(value).append('\"').append(',');
        }
        if (topicLevel != null) {
            sb.append("\"topicLevel\":");
            if (!(topicLevel).isEmpty()) {
                sb.append("{");
                final Set<?> mapKeySet = (topicLevel).keySet();
                for (Object mapKey : mapKeySet) {
                    final Object mapValue = (topicLevel).get(mapKey);
                    sb.append("\"").append(mapKey).append("\":\"").append(Objects.toString(mapValue, "")).append("\",");
                }
                sb.replace(sb.length() - 1, sb.length(), "}");
            } else {
                sb.append("{}");
            }
            sb.append(',');
        }
        if (subscribeNodes != null) {
            sb.append("\"subscribeNodes\":");
            if (!(subscribeNodes).isEmpty()) {
                sb.append("[");
                for (Object collectionValue : subscribeNodes) {
                    sb.append("\"").append(Objects.toString(collectionValue, "")).append("\",");
                }
                sb.replace(sb.length() - 1, sb.length(), "]");
            } else {
                sb.append("[]");
            }
            sb.append(',');
        }
        if (offlineSessions != null) {
            sb.append("\"offlineSessions\":");
            if (!(offlineSessions).isEmpty()) {
                sb.append("[");
                for (Object collectionValue : offlineSessions) {
                    sb.append("\"").append(Objects.toString(collectionValue, "")).append("\",");
                }
                sb.replace(sb.length() - 1, sb.length(), "]");
            } else {
                sb.append("[]");
            }
            sb.append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    @Data
    public static class SubscriptionPO {

        private String clientIdentifier;
        private byte qos;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("{");
            if (clientIdentifier != null) {
                sb.append("\"clientIdentifier\":\"").append(clientIdentifier).append('\"').append(',');
            }
            sb.append("\"qos\":").append(qos).append(',');
            return sb.replace(sb.length() - 1, sb.length(), "}").toString();
        }

    }


}
