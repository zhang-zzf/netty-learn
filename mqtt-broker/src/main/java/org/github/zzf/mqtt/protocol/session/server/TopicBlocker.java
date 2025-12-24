package org.github.zzf.mqtt.protocol.session.server;

import java.util.concurrent.CompletableFuture;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2025-12-23
 */
public interface TopicBlocker {

    /**
     * return the first Topic that match the TopicName;
     */
    Topic match(String topicName);

    CompletableFuture<Void> add(String... topicFilter);

    CompletableFuture<Void> del(String... topicFilter);
}
