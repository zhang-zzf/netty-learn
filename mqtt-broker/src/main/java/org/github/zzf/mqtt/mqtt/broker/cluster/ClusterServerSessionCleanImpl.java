package org.github.zzf.mqtt.mqtt.broker.cluster;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.mqtt.broker.node.DefaultServerSession;
import org.github.zzf.mqtt.protocol.model.Connect;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 * <p>
 * Use with ClusterBroker for cleanSession=1 ServerSession in the Cluster Mode
 * <p>be careful when use for cleanSession=0 session in the Cluster Mode</p>
 */
@Slf4j
public class ClusterServerSessionCleanImpl extends DefaultServerSession implements ClusterServerSession{

    public ClusterServerSessionCleanImpl(Connect connect, Channel channel, Broker broker) {
        super(connect, channel, broker);
        // NodeServerSession only used for CleanSession
        assert connect.cleanSession();
        // for now, only used in cluster environment
        assert broker instanceof ClusterBroker;
    }

    @Override
    public ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }

    @Override
    public String nodeId() {
        return broker().nodeId();
    }

    @Override
    public boolean isOnline() {
        // always true
        return true;
    }

    @Override
    public String toString() {
        // todo
        return "todo";
    }

}
