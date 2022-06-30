package org.example.mqtt.broker;

import lombok.Data;
import org.example.mqtt.model.*;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/26
 */
@Data
public class ControlPacketContext {

    public static final int PUB_REC = 1 << 2;
    public static final int PUB_REL = 1 << 3;
    public static final int PUB_COMP = 1 << 4;
    public static final int PUB_ACK = 1 << 5;

    public static final int RECEIVED = 1 << 6;
    public static final int ONWARD = 1 << 7;

    public static final int CREATED = 1 << 10;
    public static final int SENDING = 1 << 11;
    public static final int SENT = 1 << 12;

    private AtomicInteger status;
    private long markedMillis;
    private Publish packet;

    private ControlPacketContext next;

    public ControlPacketContext(Publish packet, int status) {
        this.packet = packet;
        this.status = new AtomicInteger(status);
        this.markedMillis = System.currentTimeMillis();
    }

    public Publish packet() {
        return packet;
    }

    public ControlPacket pubAck() {
        return new PubAck(packet().packetIdentifier());
    }

    public boolean canPublish() {
        return status.get() == CREATED;
    }


    public ControlPacketContext markStatus() {
        return markStatus(status.get());
    }

    public ControlPacketContext markStatus(int expect) {
        int update = expect;
        if (expect == RECEIVED) {
            update = ONWARD;
        } else if (expect == ONWARD) {
            if (packet().atLeastOnce()) {
                update = PUB_ACK;
            }
            if (packet().exactlyOnce()) {
                update = PUB_REC;
            }
        } else if (expect == PUB_REC) {
            update = PUB_REL;
        } else if (expect == PUB_REL) {
            update = PUB_COMP;
        } else if (expect == SENT) {
            if (packet().atLeastOnce()) {
                update = PUB_ACK;
            }
            if (packet().exactlyOnce()) {
                update = PUB_REC;
            }
        }
        return markStatus(expect, update);
    }

    public ControlPacketContext markStatus(int expect, int update) {
        if (!status.compareAndSet(expect, update)) {
            throw new IllegalStateException();
        }
        this.markedMillis = System.currentTimeMillis();
        return this;
    }

    public boolean complete() {
        int s = status.get();
        if (packet().atMostOnce()) {
            if (s == ONWARD || s == SENT) {
                return true;
            }
        }
        if (packet().atLeastOnce() && s == PUB_ACK) {
            return true;
        }
        if (packet().exactlyOnce() && s == PUB_COMP) {
            return true;
        }
        return false;
    }

    public boolean inSending() {
        return status.get() == SENDING;
    }

    public ControlPacketContext next() {
        return this.next;
    }

    public ControlPacket pubRec() {
        return new PubRec(packet().packetIdentifier());
    }

    public ControlPacket pubRel() {
        return new PubRel(packet().packetIdentifier());
    }

    public ControlPacket pubComp() {
        return new PubComp(packet().packetIdentifier());
    }

    public ControlPacket retryPacket() {
        int s = status.get();
        if (packet().atLeastOnce()) {
            switch (s) {
                case ONWARD:
                    // receive message case
                    return pubAck();
                case SENT:
                    // send message case
                    return packet();
                default:
            }
        }
        if (packet().exactlyOnce()) {
            switch (s) {
                // receive message case
                case ONWARD:
                    return pubRec();
                // receive message case
                case PUB_REL:
                    return pubComp();
                // send message case
                case SENT:
                    return packet();
                // send message case
                case PUB_REC:
                    return pubRel();
                default:
            }
        }
        return null;
    }

}
