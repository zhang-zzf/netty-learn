package org.github.zzf.mqtt.protocol.session.server;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2025-12-21
 */
public interface RoutingTable {

    CompletableFuture<Void> subscribe(String clientId, Collection<Subscription> subscription);

    CompletableFuture<Void> unsubscribe(String clientId, Collection<Subscription> subscription);

    List<Topic> match(String topicName);

    /**
     * 查询 Topic
     */
    Optional<Topic> topic(String topicFilter);
}
