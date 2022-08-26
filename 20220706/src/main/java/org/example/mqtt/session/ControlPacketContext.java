package org.example.mqtt.session;

import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.example.mqtt.session.ControlPacketContext.Status.*;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/26
 */
@Slf4j
public class ControlPacketContext {

    public String pId() {
        return packet.pId();
    }

    public enum Status {
        INIT,
        HANDLED,
        SENDING,
        SENT,
        PUB_REC,
        PUB_REL,
        PUB_COMP,
        PUB_ACK,
        ;
    }

    public enum Type {
        IN,
        OUT,
        ;
    }

    private final Publish packet;
    private final Type type;
    private final AtomicReference<Status> status;
    private final Promise<?> promise;
    private long markedMillis;
    private int retryTimes;

    private ControlPacketContext next;

    public ControlPacketContext(Publish packet, Status status, Type type, Promise<Void> promise) {
        this.packet = packet;
        this.status = new AtomicReference<>(status);
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

    public ControlPacketContext markStatus(Status expect, Status update) {
        if (!status.compareAndSet(expect, update)) {
            throw new IllegalStateException();
        }
        this.markedMillis = System.currentTimeMillis();
        return this;
    }

    public boolean complete() {
        Status s = status.get();
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
        Status s = status.get();
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
        sb.append("\"type\":\"").append(type.name()).append("\",");
        if (status != null) {
            sb.append("\"status\":\"").append(status.get().name()).append("\",");
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
