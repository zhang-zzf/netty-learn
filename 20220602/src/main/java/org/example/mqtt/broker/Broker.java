package org.example.mqtt.broker;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/24
 */
public interface Broker {

    /**
     * publish message
     *
     * @param message message
     */
    void publish(Message message);

    /**
     * register a session to the broker
     *
     * @param session session
     */
    void register(Session session);

    /**
     * deregister a session from the broker
     *
     * @param session session
     */
    void deregister(Session session);

}
