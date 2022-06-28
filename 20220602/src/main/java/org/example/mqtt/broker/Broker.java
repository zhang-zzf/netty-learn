package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.Map;

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
     * @throws Exception
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
     * @param subscribe the topic and the qos
     * @return subscription
     */
    Map<Topic.TopicFilter, Subscription> register(Session session, Subscribe subscribe);

    /**
     * deregister a subscription between the session and the topic
     *
     * @param session session
     * @param subscribe the topic and the qos
     */
    void deregister(Session session, Subscribe subscribe);

}
