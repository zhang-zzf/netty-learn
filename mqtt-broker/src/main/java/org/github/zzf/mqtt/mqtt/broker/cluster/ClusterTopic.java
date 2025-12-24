package org.github.zzf.mqtt.mqtt.broker.cluster;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.github.zzf.mqtt.protocol.session.server.Topic;
import org.github.zzf.mqtt.mqtt.broker.node.DefaultTopic;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Getter
@Setter
@Accessors(chain = true)
public class ClusterTopic extends DefaultTopic implements Topic {

    /**
     * 订阅此 Topic 的 Node
     */
    private Set<String> nodes;
    /**
     * 订阅此 Topic 的离线 Session
     * <p>ClientIdentifier <-> QoS</p>
     */
    private Map<String, Integer> offlineSessions;

    public ClusterTopic(String topicFilter) {
        super(topicFilter);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (nodes != null) {
            sb.append("\"nodes\":");
            if (!(nodes).isEmpty()) {
                sb.append("[");
                for (Object collectionValue : nodes) {
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
                sb.append("{");
                final Set<?> mapKeySet = (offlineSessions).keySet();
                for (Object mapKey : mapKeySet) {
                    final Object mapValue = (offlineSessions).get(mapKey);
                    sb.append("\"").append(mapKey).append("\":\"").append(Objects.toString(mapValue, "")).append("\",");
                }
                sb.replace(sb.length() - 1, sb.length(), "}");
            } else {
                sb.append("{}");
            }
            sb.append(',');
        }
        if (topicFilter() != null) {
            sb.append("\"topicFilter\":\"").append(topicFilter()).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
