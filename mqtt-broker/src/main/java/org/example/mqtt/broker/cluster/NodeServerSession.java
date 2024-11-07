package org.example.mqtt.broker.cluster;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.node.DefaultServerSession;
import org.example.mqtt.model.Connect;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 *
 * Use with ClusterBroker for cleanSession=1 ServerSession in the Cluster Mode
 * <p>be careful when use for cleanSession=0 session in the Cluster Mode</p>
 */
@Slf4j
public class NodeServerSession extends DefaultServerSession {

    private boolean topicCleared = false;

    public NodeServerSession(Connect connect, Channel channel, Broker broker) {
        super(connect, channel, broker);
    }

    @Override
    public void close() {
        super.close();
        if (!topicCleared) {
            // todo
            broker().detachSession(this,false);
            topicCleared = true;
        }
    }

    @Override
    public ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }

    @Override
    public String toString() {
        // todo
        return "todo";
    }

}
