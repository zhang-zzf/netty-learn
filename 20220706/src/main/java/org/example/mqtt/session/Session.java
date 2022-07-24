package org.example.mqtt.session;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
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
     * send a message to client
     *
     * @param message message
     */
    Future<Void> send(ControlPacket message);

    /**
     * receive a message from client
     *
     * @param message message
     */
    void messageReceived(ControlPacket message);

    /**
     * the channel that the session bind to
     *
     * @return Channel
     */
    Channel channel();

    /**
     * bind the session to a channel
     *
     * @param channel Channel use to send and receive data from pair
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
     * @return List<Subscription>
     */
    Set<Subscribe.Subscription> subscriptions();

    /**
     * next packetIdentifier to use
     *
     * @return packetIdentifier
     */
    short nextPacketIdentifier();

    /**
     * whether the session is cleanSession
     *
     * @return true if session is CleanSession otherwise false
     */
    boolean cleanSession();

}
