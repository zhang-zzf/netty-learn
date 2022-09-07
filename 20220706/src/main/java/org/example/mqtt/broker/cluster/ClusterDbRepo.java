package org.example.mqtt.broker.cluster;

import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.session.ControlPacketContext;

import java.util.List;

public interface ClusterDbRepo {

    ServerSession querySessionByClientIdentifier(String clientIdentifier);

    boolean offerToSessionQueue(ClusterControlPacketContext tail,
                                ClusterControlPacketContext cpx);

    List<ClusterControlPacketContext> fetchFromSessionQueue(String clientIdentifier,
                                                            ClusterDbQueue.Type type,
                                                            boolean tail,
                                                            int size);

    void updateSessionQueueStatus(String clientIdentifier, ControlPacketContext.Type type,
                                  String packetIdentifier,
                                  ControlPacketContext.Status expect,
                                  ControlPacketContext.Status update);

    ClusterControlPacketContext findFromSessionQueue(String clientIdentifier,
                                                     ClusterDbQueue.Type inQueue,
                                                     short packetIdentifier);

    void deleteFromSessionQueue(ClusterControlPacketContext cpx);

}
