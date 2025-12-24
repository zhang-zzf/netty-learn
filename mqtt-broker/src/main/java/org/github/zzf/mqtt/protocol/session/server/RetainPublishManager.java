package org.github.zzf.mqtt.protocol.session.server;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.github.zzf.mqtt.protocol.model.Publish;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2025-12-24
 */
public interface RetainPublishManager {

    CompletableFuture<List<Publish>> match(String... topicFilter);

    CompletableFuture<Void> add(Publish... packets);

    CompletableFuture<Void> del(Publish... packets);
}
