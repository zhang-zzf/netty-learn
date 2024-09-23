package org.example.mqtt.broker;

import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;

import javax.annotation.Nullable;
import java.util.*;

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
    int forward(Publish packet);

    void connect(ServerSession session);

    /**
     * register a subscription between the session and the topic
     */
    List<Subscribe.Subscription> subscribe(ServerSession session, Subscribe subscribe);

    /**
     * deregister a subscription between the session and the topic
     */
    void unsubscribe(ServerSession session, Unsubscribe packet);

    Optional<Topic> topic(String topicFilter);

    Set<Integer> supportProtocolLevel();

    /**
     * find retain PublishPacket that match the topicFilter
     *
     * @param topicFilter TopicFilter
     * @return matched PublishPacket List
     */
    List<Publish> retainMatch(String topicFilter);

    /**
     * Broker 处理接受到的 Publish
     *
     * @param packet ControlPacket
     */
    void handlePublish(Publish packet);

    ServerSession createSession(Connect connect);

    @Nullable
    ServerSession session(String clientIdentifier);

    Map<String, ServerSession> sessionMap();

    void destroySession(ServerSession session);

    default boolean closed() {
        return false;
    }

    boolean block(Publish packet);

    void closeSession(ServerSession session);
}
