package org.example.mqtt.broker.cluster;

import org.example.mqtt.session.ControlPacketContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public interface ClusterDbRepo {

    void saveSession(ClusterServerSession session);

    ClusterServerSession getSession(String clientIdentifier);

    void deleteSession(ClusterServerSession session);

    boolean offerCpx(@Nullable ClusterControlPacketContext tail,
                     ClusterControlPacketContext cpx);

    @Nullable
    ClusterControlPacketContext getCpx(String clientIdentifier,
                                       ControlPacketContext.Type inQueue,
                                       short packetIdentifier);

    List<ClusterControlPacketContext> searchCpx(String clientIdentifier,
                                                ControlPacketContext.Type type,
                                                boolean tail,
                                                int size);

    void updateCpxStatus(ClusterControlPacketContext cpx);

    /**
     * 从 cpx 队列中移除 cpx
     * <p>隐含：必须从 Queue 队头移除。若 cpx 不是队头，移除失败</p>
     *
     * @param cpx data
     * @return true / false
     * @throws IllegalStateException if cpx is not the head of the Queue
     */
    boolean deleteCpx(ClusterControlPacketContext cpx);

    void addNodeToTopic(String nodeId, List<String> tfSet);

    void removeNodeFromTopic(String nodeId, List<String> tfSet);

    void removeTopic(List<String> tfSet);

    boolean offerToOutQueueOfTheOfflineSession(ClusterServerSession s, ClusterControlPacketContext ccpx);

    List<ClusterTopic> matchTopic(String topicName);

    void close() throws IOException;

}
