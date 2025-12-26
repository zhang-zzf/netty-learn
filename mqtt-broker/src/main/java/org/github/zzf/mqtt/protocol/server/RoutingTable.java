package org.github.zzf.mqtt.protocol.server;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2025-12-21
 */
public interface RoutingTable extends AutoCloseable {

    CompletableFuture<Void> subscribe(String clientId,
            Collection<Subscription> subscription);

    CompletableFuture<Void> unsubscribe(String clientId,
            Collection<Subscription> subscription);

    List<Topic> match(String topicName);

    /**
     * 查询 Topic
     */
    Optional<Topic> topic(String topicFilter);
}
