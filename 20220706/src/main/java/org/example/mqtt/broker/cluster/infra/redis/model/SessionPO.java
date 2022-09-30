package org.example.mqtt.broker.cluster.infra.redis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.model.Subscribe;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Data
@Accessors(chain = true)
public class SessionPO {

    /**
     * clientIdentifier
     */
    private String cId;
    /**
     * 订阅
     */
    private Set<SubscriptionPO> sub;
    private String nodeId;
    /**
     * Session 离线后 outQueue.tail 的 packetIdentifier
     */
    private Short oQPId;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
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
        if (oQPId != null) {
            sb.append("\"oQPID\":").append(oQPId).append(',');
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

    public ClusterServerSession toDomain() {
        return toDomain(this);
    }

    public static ClusterServerSession toDomain(SessionPO po) {
        if (po == null) {
            return null;
        }
        Set<Subscribe.Subscription> subscriptions = new HashSet<>();
        Set<SessionPO.SubscriptionPO> sPO = po.getSub();
        if (sPO != null) {
            subscriptions = sPO.stream()
                    .map(o -> new Subscribe.Subscription(o.getTf(), o.getQos()))
                    .collect(toSet());
        }
        return ClusterServerSession.from(po.getCId(), po.getNodeId(), subscriptions, po.getOQPId());
    }

    public static SessionPO fromDomain(ClusterServerSession domain) {
        Set<SessionPO.SubscriptionPO> subscriptions = null;
        if (!domain.subscriptions().isEmpty()) {
            subscriptions = domain.subscriptions().stream()
                    .map(s -> new SubscriptionPO(s.topicFilter(), s.qos()))
                    .collect(toSet());
        }
        SessionPO po = new SessionPO()
                .setCId(domain.clientIdentifier())
                .setNodeId(domain.nodeId())
                .setSub(subscriptions)
                .setOQPId(domain.outQueuePacketIdentifier());
        return po;
    }

}
