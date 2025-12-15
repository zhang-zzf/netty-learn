package org.github.zzf.mqtt.mqtt.broker;

import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;

import java.util.*;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public interface Broker {

    /**
     * onward message
     *
     * @param packet data
     */
    int forward(Publish packet);

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

    boolean attachSession(ServerSession session);

    void detachSession(ServerSession session, boolean force);

    ServerSession session(String clientIdentifier);

    Map<String, ServerSession> sessionMap();

    void close() throws Exception;

    // todo
    default boolean closed() {
        return false;
    }

    boolean block(Publish packet);

}
