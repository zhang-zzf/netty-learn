package org.example.mqtt.session;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.Set;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Subscribe;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public interface Session {

    /**
     * clientIdentifier
     *
     * @return clientIdentifier
     */
    String clientIdentifier();

    /**
     * send a message to peer
     *
     * @param message message
     */
    ChannelFuture send(ControlPacket message);

    /**
     * receive a message from peer
     *
     * @param message message
     */
    void onPacket(ControlPacket message);

    /**
     * the channel that the session bind to
     *
     * @return Channel
     */
    Channel channel();

    /**
     * session is active
     */
    boolean isActive();

    /**
     * the Subscribe that the session was registered
     *
     * @return Set<Subscription>
     */
    Set<Subscribe.Subscription> subscriptions();

    /**
     * next packetIdentifier to use
     *
     * @return packetIdentifier
     */
    short nextPacketIdentifier();

    /**
     * close Session
     */
    void close();

    /**
     * whether the session is cleanSession
     *
     * @return true if session is CleanSession otherwise false
     */
    boolean cleanSession();

    Session migrate(Session session);

}
