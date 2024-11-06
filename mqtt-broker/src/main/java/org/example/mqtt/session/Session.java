package org.example.mqtt.session;

import io.netty.channel.Channel;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Subscribe;

import java.util.Set;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public interface Session extends AutoCloseable {

    /**
     * clientId
     *
     * @return clientId
     */
    String clientIdentifier();

    /**
     * send a message to peer
     * <p>Sender send a message to Receiver</p>
     *
     * @param message message
     */
    void send(ControlPacket message);

    /**
     * receive a message from peer
     * <p>Receiver receive a message from Sender</p>
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

    boolean isActive();

    /**
     * todo
     * bind the session to a channel
     *
     * @param channel Channel that use to send and receive data from pair
    void bind(Channel channel);
     */

    /**
     * todo
     * whether the Session is bound with a Channel
     *
     * @return Returns {@code true} if the {@link Session} is bound with a {@link Channel}.
    boolean isBound();
     */

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

    void migrate(Session session);

}
