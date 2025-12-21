package org.github.zzf.mqtt.mqtt.broker.cluster;

import org.github.zzf.mqtt.protocol.session.server.ServerSession;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2024-11-06
 */
public interface ClusterServerSession extends ServerSession {

    /**
     * the broker that session was bound to
     *
     * @return Broker
     */
    ClusterBroker broker();

    String nodeId();

    boolean isOnline();

}
