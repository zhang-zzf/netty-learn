package org.example.mqtt.broker.cluster;

import org.example.mqtt.model.Publish;
import org.example.mqtt.session.ControlPacketContext;

public class ClusterControlPacketContext extends ControlPacketContext {

    private final ClusterDbRepo clusterDbRepo;
    private final String clientIdentifier;
    private Short nextPacketIdentifier;

    public ClusterControlPacketContext(ClusterDbRepo clusterDbRepo,
                                       String clientIdentifier,
                                       Type type,
                                       Publish packet, Status status, Short nextPacketIdentifier) {
        super(packet, status, type);
        this.clusterDbRepo = clusterDbRepo;
        this.clientIdentifier = clientIdentifier;
        this.nextPacketIdentifier = nextPacketIdentifier;
    }

    @Override
    public ClusterControlPacketContext markStatus(Status expect, Status update) {
        clusterDbRepo.updateCpxStatus(clientIdentifier, type(), pId(), expect, update);
        super.markStatus(expect, update);
        return this;
    }

    public String id() {
        return id(this);
    }

    public static String id(ClusterControlPacketContext ccpx) {
        return id(ccpx.clientIdentifier, ccpx.type(), ccpx.packetIdentifier());
    }

    public static String id(String clientIdentifier, Type type, short packetIdentifier) {
        return clientIdentifier + "_" + type + "_" + packetIdentifier;
    }

    public static Integer packetIdentifier(String id) {
        if (id == null) {
            return null;
        }
        // todo mqtt_2_1_IN_1555
        // clientIdentifier has _
        throw new UnsupportedOperationException();
        // return Integer.valueOf(id.split("_")[2]);
    }

    public String clientIdentifier() {
        return clientIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        if (packet() != null) {
            sb.append("\"packet\":");
            String objectStr = packet().toString().trim();
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

    public Short nextPacketIdentifier() {
        return nextPacketIdentifier;
    }

    public void nextPacketIdentifier(short packetIdentifier) {
        this.nextPacketIdentifier = packetIdentifier;
    }

}