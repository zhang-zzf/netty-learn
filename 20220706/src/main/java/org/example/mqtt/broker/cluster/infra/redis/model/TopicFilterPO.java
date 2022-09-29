package org.example.mqtt.broker.cluster.infra.redis.model;

import lombok.Data;
import lombok.experimental.Accessors;
import org.example.mqtt.broker.cluster.ClusterTopic;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

/**
 * 集群级别 TopicFilter 模型
 * <p> "topic_abc_de" use as ID </p>
 */
/*
 {
  "settings": {
    "index.number_of_shards": 3,
    "index.number_of_replicas": 1
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "1": {
        "type": "keyword"
      },
      "2": {
        "type": "keyword"
      },
      "3": {
        "type": "keyword"
      },
      "4": {
        "type": "keyword"
      },
      "5": {
        "type": "keyword"
      },
      "6": {
        "type": "keyword"
      },
      "7": {
        "type": "keyword"
      },
      "8": {
        "type": "keyword"
      },
      "id": {
        "type": "keyword"
      },
      "nodes": {
        "type": "keyword"
      },
      "offlineSubscriptions": {
        "properties": {
          "clientId": {
            "type": "keyword"
          },
          "qos": {
            "type": "byte"
          }
        }
      }
    }
  }
}
 */
@Data
@Accessors(chain = true)
public class TopicFilterPO {

    /**
     * topic/abc/de
     */
    private String value;
    /**
     * "0" -> "topic"
     * "1" -> "abc"
     * "2" -> "de"
     */
    private Map<String, String> topicLevel;
    /**
     * 订阅此 TopicFilter 的 node 节点
     */
    private Set<String> nodes;
    /**
     * 订阅此 TopicFilter 的离线 Session
     */
    private Set<SubscriptionPO> offlineSessions;

    public TopicFilterPO() {

    }

    public TopicFilterPO(String tf, String... nodes) {
        this.value = tf;
        this.topicLevel = new HashMap<>(8);
        String[] level = tf.split("/");
        for (int i = 0; i < level.length; i++) {
            topicLevel.put(String.valueOf(i), level[i]);
        }
        this.nodes = new HashSet<>();
        for (String node : nodes) {
            this.nodes.add(node);
        }
    }

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
        if (nodes != null) {
            sb.append("\"subscribeNodes\":");
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SubscriptionPO that = (SubscriptionPO) o;
            return getClientIdentifier().equals(that.getClientIdentifier());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClientIdentifier());
        }

    }

    public static ClusterTopic toDomain(TopicFilterPO po) {
        ClusterTopic ret = new ClusterTopic(po.getValue());
        ret.setNodes(po.getNodes());
        if (po.getOfflineSessions() != null) {
            Map<String, Byte> map = po.getOfflineSessions()
                    .stream()
                    .collect(toMap(s -> s.getClientIdentifier(), s -> s.getQos()));
            ret.setOfflineSessions(map);
        } else {
            ret.setOfflineSessions(emptyMap());
        }
        return ret;
    }

}
