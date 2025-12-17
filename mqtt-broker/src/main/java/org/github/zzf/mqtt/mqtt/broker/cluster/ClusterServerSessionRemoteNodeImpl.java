package org.github.zzf.mqtt.mqtt.broker.cluster;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import java.util.Collections;
import java.util.Set;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.session.Session;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2024-11-06
 */
public class ClusterServerSessionRemoteNodeImpl implements ClusterServerSession {

    private final String clientIdentifier;

    private final String nodeId;

    private final Set<Subscribe.Subscription> subscriptions;

    private final Short nextPacketIdentifier;

    public ClusterServerSessionRemoteNodeImpl(String clientIdentifier, String nodeId, Set<Subscription> subscriptions, Short nextPacketId) {
        this.clientIdentifier = clientIdentifier;
        this.nodeId = nodeId;
        this.subscriptions = subscriptions;
        this.nextPacketIdentifier = nextPacketId;
    }

    @Override
    public short nextPacketIdentifier() {
        return nextPacketIdentifier;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public String clientIdentifier() {
        return clientIdentifier;
    }

    @Override
    public Set<Subscription> subscriptions() {
        return Collections.unmodifiableSet(subscriptions);
    }

    /**
     * the broker that session was bound to
     *
     * @return Broker
     */
    public ClusterBroker broker() {
        return null;
    }

    @Override
    public ChannelPromise send(ControlPacket message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onPacket(ControlPacket message) {
        throw new UnsupportedOperationException();

    }

    @Override
    public Channel channel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isActive() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cleanSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterServerSessionRemoteNodeImpl migrate(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOnline() {
        throw new UnsupportedOperationException();
    }

}
