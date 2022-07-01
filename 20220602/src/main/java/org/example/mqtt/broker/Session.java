package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.model.ControlPacket;

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
    void send(ControlPacket message);

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

}
