package org.example.mqtt.broker;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.mqtt.model.*;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    Map<String, Broker.ListenPort> listenedServer();

    /**
     * 处理 Broker 接受到的 Publish
     *
     * @param packet ControlPacket
     */
    void handlePublish(Publish packet);

    ServerSession createSession(Connect connect);

    @Nullable
    ServerSession session(String clientIdentifier);

    void destroySession(ServerSession session);

    default boolean closed() {
        return false;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class ListenPort {

        // mqtt://host:port
        private String url;
        private Channel channel;

    }

}
