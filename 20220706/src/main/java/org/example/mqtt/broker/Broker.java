package org.example.mqtt.broker;

import org.example.mqtt.session.Session;
import org.example.mqtt.model.Publish;

import java.util.List;
import java.util.Set;

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

    ServerSession session(String clientIdentifier);

    /**
     * deregister a session from the broker
     *
     * @param session Session
     */
    void disconnect(Session session);

    /**
     * register a subscription between the session and the topic
     *
     * @param subscriptions the topic and the qos
     */
    List<Subscription> register(List<Subscription> subscriptions);

    /**
     * deregister a subscription between the session and the topic
     *
     * @param subscriptions the topic and the qos
     */
    void deregister(List<Subscription> subscriptions);

    Set<Integer> supportProtocolLevel();

    void connect(ServerSession session);

}