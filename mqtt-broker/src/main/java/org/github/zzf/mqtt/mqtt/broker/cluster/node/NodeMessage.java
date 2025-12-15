package org.github.zzf.mqtt.mqtt.broker.cluster.node;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static io.netty.buffer.ByteBufUtil.getBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

@Data
@Accessors(chain = true)
public class NodeMessage {

    public static final String ACTION_PUBLISH_FORWARD = "Publish.forward";
    public static final String ACTION_SESSION_CLOSE = "Session.Close";
    public static final String ACTION_BROKER_CLOSE = "Broker.Close";
    public static final String ACTION_TOPIC_QUERY = "Topic.Query";
    public static final String INFO_CLUSTER_NODES = "Cluster.Nodes";
    public static final String INFO_CLIENT_CONNECT = "Client.Connect";
    private String nodeId;
    private String packet;
    private String payload;

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

    public static NodeMessage wrapClusterNodes(String localNodeId, Set<NodeInfo> nodes) {
        NodeMessage nm = new NodeMessage();
        nm.setNodeId(localNodeId);
        nm.setPacket(INFO_CLUSTER_NODES);
        nm.setPayload(JSON.toJSONString(nodes));
        return nm;
    }

    public Set<NodeInfo> unwrapClusterNodes() {
        return new HashSet<>(JSON.parseArray(payload, NodeInfo.class));
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
        return JSON.parseObject(new String(payload, UTF_8), NodeMessage.class);
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

    @Data
    @Accessors(chain = true)
    public static class NodeInfo {

        private String id;
        private String address;
        private Set<String> nodeClientIds;
        private String cmNodeClientId;
        private Set<String> subscribers;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("{");
            if (id != null) {
                sb.append("\"id\":\"").append(id).append('\"').append(',');
            }
            if (address != null) {
                sb.append("\"address\":\"").append(address).append('\"').append(',');
            }
            if (nodeClientIds != null) {
                sb.append("\"nodeClientIds\":");
                if (!(nodeClientIds).isEmpty()) {
                    sb.append("[");
                    for (Object collectionValue : nodeClientIds) {
                        sb.append("\"").append(Objects.toString(collectionValue, "")).append("\",");
                    }
                    sb.replace(sb.length() - 1, sb.length(), "]");
                } else {
                    sb.append("[]");
                }
                sb.append(',');
            }
            if (cmNodeClientId != null) {
                sb.append("\"cmNodeClientId\":\"").append(cmNodeClientId).append('\"').append(',');
            }
            if (subscribers != null) {
                sb.append("\"subscribers\":");
                if (!(subscribers).isEmpty()) {
                    sb.append("[");
                    for (Object collectionValue : subscribers) {
                        sb.append("\"").append(Objects.toString(collectionValue, "")).append("\",");
                    }
                    sb.replace(sb.length() - 1, sb.length(), "]");
                } else {
                    sb.append("[]");
                }
                sb.append(',');
            }
            return sb.replace(sb.length() - 1, sb.length(), "}").toString();
        }

    }

}
