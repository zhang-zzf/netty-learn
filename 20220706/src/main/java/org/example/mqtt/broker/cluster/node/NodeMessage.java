package org.example.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import org.example.mqtt.model.Publish;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.netty.buffer.ByteBufUtil.getBytes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.example.mqtt.model.Publish.*;

@Data
public class NodeMessage {

    public static final String ACTION_PUBLISH_FORWARD = "Publish.forward";
    public static final String ACTION_SESSION_CLOSE = "Session.Close";
    public static final String ACTION_BROKER_CLOSE = "Broker.Close";
    public static final String INFO_CLUSTER_NODES = "Cluster.Nodes";
    public static final String INFO_CLIENT_CONNECT = "Client.Connect";
    private String nodeId;
    private String packet;
    private String payload;

    public Map<String, Object> meta;

    public static NodeMessage wrapPublish(String nodeId, Publish packet) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(ACTION_PUBLISH_FORWARD);
        nm.setPayload(Base64.getEncoder().encodeToString(getBytes(packet.toByteBuf())));
        initWrapMetricMeta(nm, packet);
        return nm;
    }

    private static void initWrapMetricMeta(NodeMessage nm, Publish packet) {
        nm.meta = new HashMap<>(4);
        if (packet.meta() != null) {
            nm.meta.putAll(packet.meta());
        }
        nm.setMetricMeta(META_NM_WRAP, System.currentTimeMillis());
    }

    public static NodeMessage fromBytes(ByteBuf payload) {
        return fromBytes(getBytes(payload));
    }

    public static NodeMessage wrapConnect(String nodeId, String clientIdentifier) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(INFO_CLIENT_CONNECT);
        nm.setPayload(clientIdentifier);
        return nm;
    }

    public Publish unwrapPublish() {
        Publish publish = new Publish(Unpooled.copiedBuffer(Base64.getDecoder().decode(getPayload())));
        // just for metric usage
        // transfer to Publish packet
        metricMeta(publish);
        return publish;
    }

    private void metricMeta(Publish publish) {
        publish.addMeta(meta);
        // mark source
        publish.addMeta(META_P_SOURCE, META_P_SOURCE_BROKER);
    }

    public static NodeMessage wrapSessionClose(String nodeId, String clientIdentifier) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(ACTION_SESSION_CLOSE);
        nm.setPayload(clientIdentifier);
        return nm;
    }

    public String unwrapSessionClose() {
        return payload;
    }

    public static NodeMessage wrapClusterNodes(String nodeId, Map<String, String> nodes) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(INFO_CLUSTER_NODES);
        nm.setPayload(JSON.toJSONString(nodes));
        return nm;
    }

    public Map<String, String> unwrapClusterNodes() {
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
        if (payload != null) {
            sb.append("\"payload\":\"").append(payload).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public static NodeMessage fromBytes(byte[] payload) {
        NodeMessage nm = JSON.parseObject(new String(payload, UTF_8), NodeMessage.class);
        nm.setMetricMeta(META_NM_RECEIVE, System.currentTimeMillis());
        return nm;
    }

    private void setMetricMeta(String name, Object value) {
        if (meta == null) {
            meta = new HashMap<>();
        }
        meta.put(name, value);
    }

    public ByteBuf toByteBuf() {
        return Unpooled.copiedBuffer(JSON.toJSONString(this).getBytes(UTF_8));
    }

    public String unwrapBrokerClose() {
        return payload;
    }

    public static NodeMessage wrapBrokerClose(String nodeId, String protocol) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(nodeId);
        nm.setPacket(ACTION_BROKER_CLOSE);
        nm.setPayload(protocol);
        return nm;
    }


}
