package org.example.mqtt.broker.cluster;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.node.DefaultServerSession;
import org.example.mqtt.model.Connect;

/**
 * Use with ClusterBroker for cleanSession=1 ServerSession in the Cluster Mode
 * <p>be careful when use for cleanSession=0 session in the Cluster Mode</p>
 */
@Slf4j
public class NodeServerSession extends DefaultServerSession {

    public NodeServerSession(String clientIdentifier) {
        super(clientIdentifier);
    }

    public static ServerSession from(Connect connect) {
        return new NodeServerSession(connect.clientIdentifier())
                .reInitWith(connect);
    }

    @Override
    public void channelClosed() {
        super.channelClosed();
        // 清除路由表
        broker().removeNodeFromTopic(subscriptions());
    }

    @Override
    public ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"type\":\"NodeServerSession\",");
        sb.append("\"registered\":").append(isRegistered()).append(',');
        sb.append("\"clientIdentifier\":\"").append(clientIdentifier()).append("\",");
        sb.append("\"cleanSession\":").append(cleanSession()).append(',');
        sb.append("\"bound\":").append(isBound()).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
