package org.example.mqtt.broker.cluster;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.DefaultServerSession;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.session.ControlPacketContext;

import java.util.Queue;

@Slf4j
public class ClusterServerSession extends DefaultServerSession {

    private final ClusterDbRepo clusterDbRepo;

    public ClusterServerSession(Connect connect, ClusterDbRepo clusterDbRepo) {
        super(connect);
        this.clusterDbRepo = clusterDbRepo;
    }

    @Override
    protected Queue<ControlPacketContext> newInQueue() {
        return new ClusterDbQueue(clusterDbRepo, cId(), ClusterDbQueue.Type.IN_QUEUE);
    }

    @Override
    protected Queue<ControlPacketContext> newOutQueue() {
        return new ClusterDbQueue(clusterDbRepo, cId(), ClusterDbQueue.Type.OUT_QUEUE);
    }

    @Override
    protected ControlPacketContext findControlPacketInOutQueue(short packetIdentifier) {
        return clusterDbRepo.findFromSessionQueue(cId(), ClusterDbQueue.Type.OUT_QUEUE, packetIdentifier);
    }

    @Override
    protected ControlPacketContext findControlPacketInInQueue(short packetIdentifier) {
        return clusterDbRepo.findFromSessionQueue(cId(), ClusterDbQueue.Type.IN_QUEUE, packetIdentifier);
    }

    @Override
    protected ControlPacketContext createNewCpx(Publish packet,
                                                ControlPacketContext.Status status,
                                                ControlPacketContext.Type type) {
        return new ClusterControlPacketContext(clusterDbRepo, cId(), type, packet, status, null);
    }

}
