package org.example.mqtt.broker.cluster;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.session.ControlPacketContext;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class ClusterDbQueue extends AbstractQueue<ControlPacketContext> {

    private final ClusterDbRepo clusterDbRepo;
    private final String clientIdentifier;
    private final Type type;
    private ClusterControlPacketContext head;
    private ClusterControlPacketContext tail;

    public ClusterDbQueue(ClusterDbRepo clusterDbRepo, String clientIdentifier, Type type) {
        this.clusterDbRepo = clusterDbRepo;
        this.clientIdentifier = clientIdentifier;
        this.type = type;
        List<ClusterControlPacketContext> fetchFromHead =
                clusterDbRepo.fetchFromSessionQueue(clientIdentifier, type, false, 1);
        this.head = fetchFromHead.isEmpty() ? null : fetchFromHead.get(0);
        List<ClusterControlPacketContext> fetchFromTail =
                clusterDbRepo.fetchFromSessionQueue(clientIdentifier, type, true, 1);
        this.tail = fetchFromTail.isEmpty() ? null : fetchFromTail.get(0);
    }

    @Override
    public Iterator<ControlPacketContext> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(ControlPacketContext cpx) {
        ClusterControlPacketContext ccpx = (ClusterControlPacketContext) cpx;
        boolean added = clusterDbRepo.offerToSessionQueue(tail, ccpx);
        // 更新 tail 指针
        if (added) {
            this.tail = ccpx;
            this.head = (this.head == null ? ccpx : this.head);
        }
        return added;
    }

    @Override
    public ClusterControlPacketContext poll() {
        ClusterControlPacketContext first = peek();
        if (first == null) {
            return null;
        }
        Short nPId = first.nextPacketIdentifier();
        if (nPId != null) {
            ClusterControlPacketContext next = clusterDbRepo.findFromSessionQueue(clientIdentifier, type, nPId);
            if (next == null) {
                // should exist
                log.warn("todo");
                this.head = null;
                this.tail = null;
            } else {
                this.head = next;
            }
        } else {
            this.head = null;
            this.tail = null;
        }
        // delete
        clusterDbRepo.deleteFromSessionQueue(first);
        return first;
    }

    @Override
    public ClusterControlPacketContext peek() {
        return head;
    }

    public enum Type {
        IN_QUEUE,
        OUT_QUEUE,
        ;
    }

}
