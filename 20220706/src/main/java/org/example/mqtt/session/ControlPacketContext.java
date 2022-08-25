package org.example.mqtt.session;

import io.netty.util.concurrent.Promise;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/26
 */
@Slf4j
public class ControlPacketContext {

    public static final int PUB_REC = 1 << 2;
    public static final int PUB_REL = 1 << 3;
    public static final int PUB_COMP = 1 << 4;
    public static final int PUB_ACK = 1 << 5;

    /**
     * when Session receive a Publish packet(may from business layer)
     */
    public static final int INIT = 1 << 6;
    /**
     *
     */
    public static final int HANDLED = 1 << 7;

    public static final int SENDING = 1 << 11;
    public static final int SENT = 1 << 12;

    public static final int OUT = 1;
    public static final int IN = 0;

    private final Publish packet;
    private final int type;
    private final AtomicInteger status;
    private final Promise<?> promise;
    private long markedMillis;
    private int retryTimes;

    private ControlPacketContext next;

    public ControlPacketContext(Publish packet, int status, int type, Promise<Void> promise) {
        this.packet = packet;
        this.status = new AtomicInteger(status);
        this.markedMillis = System.currentTimeMillis();
        this.promise = promise;
        this.type = type;
    }

    public Publish packet() {
        return packet;
    }

    public ControlPacket pubAck() {
        return PubAck.from(packet().packetIdentifier());
    }

    public boolean canPublish() {
        return status.get() == INIT;
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
            if (s == HANDLED || s == SENT) {
                return true;
            }
        }
        if (packet().atLeastOnce() && s == PUB_ACK) {
            return true;
        }
        return packet().exactlyOnce() && s == PUB_COMP;
    }

    public boolean inSending() {
        return status.get() == SENDING;
    }

    public ControlPacketContext next() {
        return this.next;
    }

    public ControlPacket pubRec() {
        return PubRec.from(packet().packetIdentifier());
    }

    public ControlPacket pubRel() {
        return PubRel.from(packet().packetIdentifier());
    }

    public ControlPacket pubComp() {
        return PubComp.from(packet().packetIdentifier());
    }

    public ControlPacket retryPacket() {
        this.retryTimes += 1;
        int s = status.get();
        Publish packet = packet();
        if (packet.atLeastOnce()) {
            switch (type) {
                case IN:
                    // receive message case
                    return pubAck();
                case OUT:
                    // send message case
                    return packet.dup(true);
                default:
            }
        }
        if (packet.exactlyOnce()) {
            switch (s) {
                case SENT:
                    // sender case
                    return packet.dup(true);
                case HANDLED:
                    // receiver case
                    return pubRec();
                case PUB_REC:
                    return pubRel();
                case PUB_REL:
                    return pubComp();
                default:
            }
        }
        throw new IllegalStateException();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (packet != null) {
            sb.append("\"packet\":");
            String objectStr = packet.toString().trim();
            if (objectStr.startsWith("{") && objectStr.endsWith("}")) {
                sb.append(objectStr);
            } else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            } else {
                sb.append("\"").append(objectStr).append("\"");
            }
            sb.append(',');
        }
        sb.append("\"type\":").append(type).append(',');
        if (status != null) {
            sb.append("\"status\":");
            String objectStr = status.toString().trim();
            if (objectStr.startsWith("{") && objectStr.endsWith("}")) {
                sb.append(objectStr);
            } else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            } else {
                sb.append("\"").append(objectStr).append("\"");
            }
            sb.append(',');
        }
        sb.append("\"markedMillis\":").append(markedMillis).append(',');
        sb.append("\"retryTimes\":").append(retryTimes).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public void timeout() {
        promise.tryFailure(new TimeoutException());
    }

    public void success() {
        promise.trySuccess(null);
    }

    public long elapseAfterLastMark() {
        return System.currentTimeMillis() - markedMillis;
    }

    public ControlPacketContext next(ControlPacketContext next) {
        this.next = next;
        return this;
    }

}
