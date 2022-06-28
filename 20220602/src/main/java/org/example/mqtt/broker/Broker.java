package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;

import java.util.List;
import java.util.Map;
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

    /**
     * register a session to the broker
     *
     * @param packet data
     * @param channel Channel
     * @return session session
     */
    Session accepted(Connect packet, Channel channel) throws Exception;

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
     * @param subscriptions the topic and the qos
     * @return subscription
     */
    Map<Topic.TopicFilter, Subscription> register(Session session, List<org.example.mqtt.model.Subscription> subscriptions);

    /**
     * deregister a subscription between the session and the topic
     *
     * @param session session
     * @param subscriptions the topic and the qos
     */
    void deregister(Session session, List<org.example.mqtt.model.Subscription> subscriptions);

    Set<Integer> supportProtocolLevel();

}
