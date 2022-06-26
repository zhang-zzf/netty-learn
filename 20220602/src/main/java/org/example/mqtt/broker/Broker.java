package org.example.mqtt.broker;

import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/24
 */
public interface Broker extends AutoCloseable {

    /**
     * onward message
     *
     * @param packet data
     */
    void onward(Publish packet);

    /**
     * register a session to the broker
     *
     * @param packet data
     * @return session session
     */
    Session accepted(Connect packet);

    /**
     * deregister a session from the broker
     *
     * @param session Session
     */
    void disconnect(Session session);

    /**
     * register a subscription between the session and the topic
     *
     * @param session session
     * @param subscribe the topic and the qos
     * @return subscription
     */
    Subscription register(Session session, Subscribe subscribe);

    /**
     * deregister a subscription between the session and the topic
     *
     * @param session session
     * @param subscribe the topic and the qos
     */
    void deregister(Session session, Subscribe subscribe);

}
