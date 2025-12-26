package org.github.zzf.mqtt.mqtt.broker.cluster.node;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.mock;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.github.zzf.mqtt.mqtt.broker.cluster.ClusterBroker;
import org.github.zzf.mqtt.mqtt.broker.cluster.ClusterBrokerImpl;
import org.github.zzf.mqtt.mqtt.broker.cluster.ClusterBrokerState;
import org.github.zzf.mqtt.server.DefaultBroker;
import org.junit.jupiter.api.Test;

class ClusterTest {

    ClusterBrokerState dbRepo = mock(ClusterBrokerState.class);
    Cluster cluster = new Cluster();
    ClusterBroker clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(packet -> 0x00), cluster);

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