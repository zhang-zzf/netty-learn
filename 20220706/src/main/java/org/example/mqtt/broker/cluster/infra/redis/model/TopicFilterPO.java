package org.example.mqtt.broker.cluster.infra.redis.model;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.experimental.Accessors;
import org.example.mqtt.broker.cluster.ClusterTopic;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * 集群级别 TopicFilter 模型
 * <p> "topic_abc_de" use as ID </p>
 */
@Data
@Accessors(chain = true)
public class TopicFilterPO {

    /**
     * topic/abc/de
     */
    private String value;
    /**
     * 订阅此 TopicFilter 的 node 节点
     */
    private Set<String> nodes;
    /**
     * 订阅此 TopicFilter 的离线 Session
     */
    private Map<String, Integer> offlineSessions;

    public TopicFilterPO() {
    }

    public TopicFilterPO(String tf) {
        this.value = tf;
    }

    public TopicFilterPO(String tf, String... nodes) {
        this.value = tf;
        this.nodes = new HashSet<>(nodes.length * 2);
        for (String node : nodes) {
            this.nodes.add(node);
        }
    }

    public static ClusterTopic toDomain(TopicFilterPO po) {
        ClusterTopic ret = new ClusterTopic(po.getValue());
        if (po.getNodes() != null) {
            ret.setNodes(po.getNodes());
        } else {
            ret.setNodes(emptySet());
        }
        if (po.getOfflineSessions() != null) {
            ret.setOfflineSessions(po.offlineSessions);
        } else {
            ret.setOfflineSessions(emptyMap());
        }
        return ret;
    }

    public static String jsonEncode(List<TopicFilterPO> list) {
        return JSON.toJSONString(list);
    }

    public static List<TopicFilterPO> jsonDecodeArray(String json) {
        return JSON.parseArray(json, TopicFilterPO.class);
    }

}
