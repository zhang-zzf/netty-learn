package org.example.mqtt.broker.cluster;

import lombok.Getter;
import lombok.Setter;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.broker.node.DefaultTopic;

import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class ClusterTopic extends DefaultTopic implements Topic {

    /**
     * 订阅此 Topic 的 Node
     */
    private Set<String> nodes;
    /**
     * 订阅此 Topic 的离线 Session
     * <p>ClientIdentifier <-> QoS</p>
     */
    private Map<String, Byte> offlineSessions;

    public ClusterTopic(String topicFilter) {
        super(topicFilter);
    }

}
