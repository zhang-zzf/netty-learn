package org.github.zzf.mqtt.mqtt.broker.cluster.infra.redis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.github.zzf.mqtt.mqtt.broker.cluster.ClusterServerSession;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Data
@Accessors(chain = true)
public class SessionPO implements Serializable {

    /**
     * clientIdentifier
     */
    private String cId;
    /**
     * 订阅
     */
    private Set<SubscriptionPO> sub;
    private String nodeId;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (cId != null) {
            sb.append("\"cId\":\"").append(cId).append('\"').append(',');
        }
        if (sub != null) {
            sb.append("\"sub\":");
            if (!(sub).isEmpty()) {
                sb.append("[");
                for (Object collectionValue : sub) {
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
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SubscriptionPO {

        private String tf;
        private int qos;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("{");
            if (tf != null) {
                sb.append("\"tf\":\"").append(tf).append('\"').append(',');
            }
            sb.append("\"qos\":").append(qos).append(',');
            return sb.replace(sb.length() - 1, sb.length(), "}").toString();
        }

    }

    public static SessionPO fromDomain(ClusterServerSession domain) {
        Set<SessionPO.SubscriptionPO> subscriptions = null;
        if (!domain.subscriptions().isEmpty()) {
            subscriptions = domain.subscriptions().stream()
                    .map(s -> new SubscriptionPO(s.topicFilter(), s.qos()))
                    .collect(toSet());
        }
        return new SessionPO()
                .setCId(domain.clientIdentifier())
                .setNodeId(domain.nodeId())
                .setSub(subscriptions);
    }

}
