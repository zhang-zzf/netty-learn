package org.github.zzf.mqtt.mqtt.broker.cluster;

import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.mqtt.session.ControlPacketContext;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.List;

/**
 * 非线程安全
 * <p>集群级别同一时刻只能有一个线程访问</p>
 */
@Slf4j
public class ClusterQueue extends AbstractQueue<ControlPacketContext> {

    private final ClusterBrokerState state;
    private final String clientIdentifier;
    private final ControlPacketContext.Type type;
    private ClusterControlPacketContext head;
    private ClusterControlPacketContext tail;

    public ClusterQueue(ClusterBrokerState state, String clientIdentifier, ControlPacketContext.Type type) {
        this.state = state;
        this.clientIdentifier = clientIdentifier;
        this.type = type;
        List<ClusterControlPacketContext> fetchFromHead = state.searchCpx(clientIdentifier, type, false, 1);
        this.head = fetchFromHead.isEmpty() ? null : fetchFromHead.get(0);
        List<ClusterControlPacketContext> fetchFromTail = state.searchCpx(clientIdentifier, type, true, 1);
        this.tail = fetchFromTail.isEmpty() ? null : fetchFromTail.get(0);
        // 若 Queue 中仅有一个元素, head 和 tail 指针必须执行同一个对象
        if (head != null && head.packetIdentifier() == tail.packetIdentifier()) {
            this.tail = head;
        }
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
        boolean added = state.offerCpx(tail, ccpx);
        // 更新 tail 指针
        if (added) {
            if (this.tail != null) {
                this.tail.nextPacketIdentifier(ccpx.packetIdentifier());
            }
            this.tail = ccpx;
            this.head = (this.head == null ? ccpx : this.head);
        } else {
            log.error("offer failed-> {}", cpx);
        }
        return true;
    }

    @Override
    public ClusterControlPacketContext poll() {
        log.debug("Queue({}/{}) poll", cId(), type);
        ClusterControlPacketContext first = peek();
        if (first == null) {
            return null;
        }
        Short nPId = first.nextPacketIdentifier();
        if (nPId != null) {
            log.debug("Queue({}/{}) fetch next from db", cId(), type);
            ClusterControlPacketContext next = state.getCpx(clientIdentifier, type, nPId);
            if (next == null) {
                // should exist
                log.error("poll [should have next Item, but does not]");
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
        boolean deleted = state.deleteCpx(first);
        if (!deleted) {
            log.error("deleteCpx failed-> {}", first);
            throw new IllegalStateException();
        }
        log.debug("Queue({}/{}) delete from db->{}", cId(), type, first);
        return first;
    }

    private String cId() {
        return clientIdentifier;
    }

    @Override
    public ClusterControlPacketContext peek() {
        return head;
    }

}
