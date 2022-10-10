package org.example.mqtt.broker.cluster.node;

import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterBrokerImpl;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.node.DefaultBroker;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.mock;

class ClusterTest {

    ClusterDbRepo dbRepo = mock(ClusterDbRepo.class);
    ClusterBroker clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker());
    Cluster cluster = new Cluster();

    @Test
    void given_whenBuildPublish_when() {
        HashMap<String, String> hashMap = new HashMap<String, String>() {{
            put("node1", "mqtt://node1:1883");
            put("node2", "mqtt://node2:1883");
        }};
        NodeMessage nm = NodeMessage.wrapClusterNodes("node3", hashMap);
        Map<String, String> map = nm.unwrapClusterNodes();
        then(map).containsExactlyEntriesOf(hashMap);
    }

    @Test
    void givenNodeName_whenBuildId_then() {
        String nodeName = "node2@zzf";
        String nodeId = Cluster.clusterNodeId(nodeName);
        String[] nameAndTime = Cluster.idToNodeNameAndTimestamp(nodeId);
        then(nameAndTime[0]).isEqualTo(nodeName);
        then(Long.valueOf(nameAndTime[1])).isLessThan(System.currentTimeMillis());
    }

}