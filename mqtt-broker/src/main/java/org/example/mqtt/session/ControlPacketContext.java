package org.example.mqtt.session;

import static org.example.mqtt.session.ControlPacketContext.Status.PUB_ACK;
import static org.example.mqtt.session.ControlPacketContext.Status.PUB_COMP;

import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.PubAck;
import org.example.mqtt.model.PubComp;
import org.example.mqtt.model.PubRec;
import org.example.mqtt.model.PubRel;
import org.example.mqtt.model.Publish;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-12
 */
@Slf4j
public class ControlPacketContext {

    public enum Type {
        IN,
        OUT,
        ;
    }

    public enum Status {
        INIT,
        HANDLED,
        PUB_REC,
        PUB_REL,
        PUB_COMP,
        PUB_ACK,
        ;
    }

    private final Publish packet;
    private final AtomicReference<Status> status;
    private final Type type;

    public ControlPacketContext(Publish packet, Status status, Type type) {
        this.packet = packet;
        this.status = new AtomicReference<>(status);
        this.type = type;
    }

    public Publish packet() {
        return packet;
    }

    public ControlPacket pubAck() {
        return new PubAck(packet().packetIdentifier());
    }

    public ControlPacketContext markStatus(Status update) {
        return markStatus(status(), update);
    }

    public ControlPacketContext markStatus(Status expect, Status update) {
        if (!status.compareAndSet(expect, update)) {
            throw new IllegalStateException();
        }
        return this;
    }

    public boolean complete() {
        if (packet.atMostOnce()) {
            return true;
        }
        Status s = status();
        if (packet().atLeastOnce() && s == PUB_ACK) {
            return true;
        }
        if (packet().exactlyOnce() && s == PUB_COMP) {
            return true;
        }
        return false;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (packet != null) {
            sb.append("\"packet\":");
            String objectStr = packet.toString().trim();
            if (objectStr.startsWith("{") && objectStr.endsWith("}")) {
                sb.append(objectStr);
            }
            else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            }
            else {
                sb.append("\"").append(objectStr).append("\"");
            }
            sb.append(',');
        }
        sb.append("\"type\":\"").append(type().name()).append("\",");
        sb.append("\"status\":\"").append(status().name()).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String pId() {
        return packet.pId();
    }

    public Status status() {
        return status.get();
    }

    public Type type() {
        return type;
    }

    public short packetIdentifier() {
        return packet.packetIdentifier();
    }

}
