package org.example.mqtt.broker;

import io.netty.channel.Channel;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/23
 */
public interface Session {

    /**
     * clientId
     *
     * @return clientId
     */
    String clientId();

    /**
     * persistent
     *
     * @return true / false
     */
    boolean persistent();

    /**
     * send a message to client
     *
     * @param message message
     */
    void send(Message message);

    /**
     * receive a message from client
     *
     * @param message message
     */
    void receive(Message message);

    /**
     * channel that between the client and server
     *
     * @return Channel
     */
    Channel channel();

}
