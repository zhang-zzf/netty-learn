package org.example.mqtt.broker;

import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;

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
    void forward(Publish packet);

    ServerSession session(String clientIdentifier);

    /**
     * deregister a session from the broker
     *
     * @param session Session
     */
    void disconnect(ServerSession session);

    /**
     * register a subscription between the session and the topic
     */
    List<Subscribe.Subscription> subscribe(ServerSession session, Subscribe subscribe);

    /**
     * deregister a subscription between the session and the topic
     */
    void deregister(ServerSession session, Unsubscribe packet);

    Set<Integer> supportProtocolLevel();

    void connect(ServerSession session);

    /**
     * retain the Publish
     *
     * @param packet Publish Packet
     */
    void retain(Publish packet);

    /**
     * find retain PublishPacket that match the topicFilter
     * @param topicFilter TopicFilter
     * @return matched PublishPacket List
     */
    List<Publish> retainMatch(String topicFilter);

}
