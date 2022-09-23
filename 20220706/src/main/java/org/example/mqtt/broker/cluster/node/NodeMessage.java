package org.example.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import org.example.mqtt.model.Publish;

import java.util.Base64;
import java.util.Map;

import static io.netty.buffer.ByteBufUtil.getBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

@Data
public class NodeMessage {

    public static final String PACKET_PUBLISH = "Publish";
    public static final String PACKET_CLUSTER_NODES = "Cluster.Nodes";
    public static final String SESSION_CLOSE = "Session.Close";
    private String nodeId;
    private String packet;
    private String payload;

    public static NodeMessage wrapPublish(String nodeId, Publish packet) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(PACKET_PUBLISH);
        nm.setPayload(Base64.getEncoder().encodeToString(getBytes(packet.toByteBuf())));
        return nm;
    }

    public static NodeMessage fromBytes(ByteBuf payload) {
        return fromBytes(getBytes(payload));
    }

    public Publish unwrapPublish() {
        return new Publish(Unpooled.copiedBuffer(Base64.getDecoder().decode(getPayload())));
    }

    public static NodeMessage wrapSessionClose(String nodeId, String clientIdentifier) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(SESSION_CLOSE);
        nm.setPayload(clientIdentifier);
        return nm;
    }

    public String unwrapSessionClose() {
        return payload;
    }

    public static NodeMessage wrapClusterState(String nodeId, Map<String, String> state) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(PACKET_CLUSTER_NODES);
        nm.setPayload(JSON.toJSONString(state));
        return nm;
    }

    public Map<String, String> unwrapClusterState() {
        return JSON.parseObject(payload,
                new TypeReference<Map<String, String>>() {
                });
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (nodeId != null) {
            sb.append("\"nodeId\":\"").append(nodeId).append('\"').append(',');
        }
        if (packet != null) {
            sb.append("\"packet\":\"").append(packet).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public static NodeMessage fromBytes(byte[] payload) {
        return JSON.parseObject(new String(payload, UTF_8), NodeMessage.class);
    }

    public ByteBuf toByteBuf() {
        return Unpooled.copiedBuffer(JSON.toJSONString(this).getBytes(UTF_8));
    }

}
