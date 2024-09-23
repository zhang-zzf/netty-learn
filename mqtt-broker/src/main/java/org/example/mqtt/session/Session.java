package org.example.mqtt.session;

import io.netty.channel.Channel;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Subscribe;

import java.util.Set;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/23
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

    /**
     * bind the session to a channel
     *
     * @param channel Channel that use to send and receive data from pair
     */
    void bind(Channel channel);

    /**
     * whether the Session is bound with a Channel
     *
     * @return Returns {@code true} if the {@link Session} is bound with a {@link Channel}.
     */
    boolean isBound();

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

    /**
     * will be called after Channel was closed
     */
    void channelClosed();

    void onSessionClose();

}
