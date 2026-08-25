package org.github.zzf.mqtt.protocol.server;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public interface Broker {

    ServerSession session(String clientId);

    byte authenticate(Connect connect);

    /**
     * Connect Event
     */
    CompletionStage<Void> connect(ServerSession session);

    CompletionStage<Void> disconnect(ServerSession session);

    /**
     * Publish Event
     *
     * @param packet ControlPacket
     */
    int forward(String clientId, Publish packet);

    /**
     * register a subscription between the session and the topic
     */
    CompletionStage<List<Integer>> subscribe(ServerSession session,
            Subscribe subscribe);

    /**
     * deregister a subscription between the session and the topic
     */
    CompletionStage<List<Integer>> unsubscribe(ServerSession session,
            Unsubscribe unsubscribe);

    void close();

    CompletableFuture<Map<String, List<Publish>>> retainedPublish(String... topicFilters);
}
