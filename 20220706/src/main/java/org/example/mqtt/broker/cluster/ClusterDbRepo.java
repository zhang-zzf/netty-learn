package org.example.mqtt.broker.cluster;

import org.example.mqtt.session.ControlPacketContext;

import java.util.List;
import java.util.Map;

public interface ClusterDbRepo {

    ClusterServerSession getSessionByClientIdentifier(String clientIdentifier);

    boolean offerToSessionQueue(ClusterControlPacketContext tail,
                                ClusterControlPacketContext cpx);

    List<ClusterControlPacketContext> searchSessionQueue(String clientIdentifier,
                                                         ClusterDbQueue.Type type,
                                                         boolean tail,
                                                         int size);

    void updateCpxStatus(String clientIdentifier,
                         ControlPacketContext.Type type,
                         String packetIdentifier,
                         ControlPacketContext.Status expect,
                         ControlPacketContext.Status update);

    ClusterControlPacketContext getCpxFromSessionQueue(String clientIdentifier,
                                                       ClusterDbQueue.Type inQueue,
                                                       short packetIdentifier);

    void deleteFromSessionQueue(ClusterControlPacketContext cpx);

    void saveSession(ClusterServerSession session);

    void deleteSession(ClusterServerSession session);

    Map<String, ClusterTopic> multiGetTopicFilter(List<String> tfSet);

    void addNodeToTopic(String nodeId, List<String> tfSet);

    void removeNodeFromTopic(String nodeId, List<String> tfSet);

    boolean offerToOutQueueOfTheOfflineSession(ClusterServerSession s, ClusterControlPacketContext ccpx);

    List<ClusterTopic> matchTopic(String topicName);

}
