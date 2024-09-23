package org.example.mqtt.broker.cluster.node;

import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterBrokerImpl;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.node.DefaultBroker;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.mock;

class ClusterTest {

    ClusterDbRepo dbRepo = mock(ClusterDbRepo.class);
    ClusterBroker clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker());
    Cluster cluster = new Cluster();

    @Test
    void given_whenBuildPublish_when() {
        Set<NodeMessage.NodeInfo> set = new HashSet<NodeMessage.NodeInfo>() {{
            add(new NodeMessage.NodeInfo().setId("node1").setAddress("mqtt://node1:1883"));
        }};
        NodeMessage nm = NodeMessage.wrapClusterNodes("node3", set);
        Set<NodeMessage.NodeInfo> unwrap = nm.unwrapClusterNodes();
        then(unwrap).containsExactlyElementsOf(set);
    }

    @Test
    void givenNodeName_whenBuildId_then() {
        String nodeName = "node2@zzf";
        String nodeId = Cluster.clusterNodeId(nodeName);
        String[] nameAndTime = Cluster.idToNodeNameAndTimestamp(nodeId);
        then(nameAndTime[0]).isEqualTo(nodeName);
        String now = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        then(Long.valueOf(nameAndTime[1])).isLessThanOrEqualTo(Long.valueOf(now));
    }

}