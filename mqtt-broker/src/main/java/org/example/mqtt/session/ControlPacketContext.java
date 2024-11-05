package org.example.mqtt.session;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.concurrent.atomic.AtomicReference;

import static org.example.mqtt.session.ControlPacketContext.Status.*;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/06/26
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
        return PubAck.from(packet().packetIdentifier());
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
        Status s = status();
        if (packet().atLeastOnce() && s == PUB_ACK) {
            return true;
        }
        return packet().exactlyOnce() && s == PUB_COMP;
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
