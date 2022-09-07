package org.example.mqtt.broker.cluster.infra.es.model;

import lombok.Data;

import java.util.Objects;
import java.util.Set;

@Data
public class SessionPO {

    /**
     * use as the ID
     */
    private String clientIdentifier;
    private Set<SubscriptionPO> subscriptions;
    private String nodeId;

    private long inQueueTimestamp;
    private long outQueueTimestamp;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (clientIdentifier != null) {
            sb.append("\"clientIdentifier\":\"").append(clientIdentifier).append('\"').append(',');
        }
        if (subscriptions != null) {
            sb.append("\"subscriptions\":");
            if (!(subscriptions).isEmpty()) {
                sb.append("[");
                for (Object collectionValue : subscriptions) {
                    sb.append("\"").append(Objects.toString(collectionValue, "")).append("\",");
                }
                sb.replace(sb.length() - 1, sb.length(), "]");
            } else {
                sb.append("[]");
            }
            sb.append(',');
        }
        if (nodeId != null) {
            sb.append("\"nodeId\":\"").append(nodeId).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    @Data
    public static class SubscriptionPO {

        private String topicFilter;
        private byte qos;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("{");
            if (topicFilter != null) {
                sb.append("\"topicFilter\":\"").append(topicFilter).append('\"').append(',');
            }
            sb.append("\"qos\":").append(qos).append(',');
            return sb.replace(sb.length() - 1, sb.length(), "}").toString();
        }

    }

}
