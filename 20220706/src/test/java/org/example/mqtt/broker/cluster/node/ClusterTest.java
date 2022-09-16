package org.example.mqtt.broker.cluster.node;

import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.mock;

class ClusterTest {

    ClusterDbRepo dbRepo = mock(ClusterDbRepo.class);
    ClusterBroker clusterBroker = new ClusterBroker(dbRepo);
    Cluster cluster = new Cluster(clusterBroker);

    @Test
    void given_when_then() {
        cluster.localNode("mqtt://10.255.5.1:1883");
        Map<String, Node> nodes = cluster.nodes();
        then(nodes).hasSize(1);
    }

    @Test
    void given_whenBuildPublish_when() {
        HashMap<String, String> hashMap = new HashMap<String, String>() {{
            put("node1", "mqtt://node1:1883");
            put("node2", "mqtt://node2:1883");
        }};
        NodeMessage nm = NodeMessage.wrapClusterState("node3", hashMap);
        Map<String, String> map = nm.unwrapClusterState();
        then(map).containsExactlyEntriesOf(hashMap);
    }

}