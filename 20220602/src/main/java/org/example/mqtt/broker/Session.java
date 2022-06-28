package org.example.mqtt.broker;

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
    void send(ControlPacket message);

    /**
     * receive a message from client
     *
     * @param message message
     */
    void messageReceived(ControlPacket message);

    /**
     * the subscription qos for the topic of the session
     *
     * @param topic topic
     * @return qos
     */
    Integer subscriptionQos(Topic topic);

    /**
     * the broker that session was bound to
     *
     * @return Broker
     */
    Broker broker();

}
