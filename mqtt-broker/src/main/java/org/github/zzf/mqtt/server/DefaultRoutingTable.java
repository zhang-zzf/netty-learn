package org.github.zzf.mqtt.server;


import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.server.RoutingTable;
import org.github.zzf.mqtt.protocol.server.Topic;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2025-12-21
 */
public class DefaultRoutingTable implements RoutingTable {

    // todo metric 监控订阅的数量和统计信息
    final TopicTree tree = new TopicTree("RoutingTable");

    @Override
    public CompletableFuture<Void> subscribe(String clientId,
            Collection<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(subscriptions.stream()
                .map(s -> this.subscribe(clientId, s))
                .toArray(CompletableFuture[]::new)
        );
    }

    private CompletableFuture<Void> subscribe(String clientId,
            Subscription subscription) {
        Consumer<AtomicReference<Topic>> dataOp = (AtomicReference<Topic> data) -> {
            TopicImpl topic = (TopicImpl) data.updateAndGet(t -> {
                if (t == null) {
                    return new TopicImpl(subscription.topicFilter());
                }
                return t;
            });
            topic.subscribers.add(clientId);
        };
        return tree.add(subscription.topicFilter(), dataOp);
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String clientId,
            Collection<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(subscriptions.stream()
                .map(s -> this.unsubscribe(clientId, s))
                .toArray(CompletableFuture[]::new)
        );
    }

    private CompletableFuture<Void> unsubscribe(String clientId,
            Subscription subscription) {
        return tree.del(subscription.topicFilter(), (AtomicReference<Topic> data) -> {
            TopicImpl topic = (TopicImpl) data.get();
            if (topic != null) {
                topic.subscribers.remove(clientId);
                if (topic.subscribers.isEmpty()) {
                    // clear the data when there is no subscriber
                    data.set(null);
                }
            }
        });
    }

    @Override
    public List<Topic> match(String topicName) {
        return tree.match(topicName);
    }

    @Override
    public Optional<Topic> topic(String topicFilter) {
        return tree.data(topicFilter);
    }

    @Override
    public void close() throws Exception {
        tree.close();
    }

    /**
     * 线程安全
     */
    @RequiredArgsConstructor
    private static class TopicImpl implements Topic {

        final String tf;
        final Set<String> subscribers
                = ConcurrentHashMap.newKeySet((Integer.getInteger("TopicImpl.subscribers.default.size", 4)));

        @Override
        public String topicFilter() {
            return tf;
        }

        /**
         * 返回订阅者只读视图，共享底层并发集合。
         * 迭代为弱一致性，遍历期间内部发生增删，可能看不到最新变更，不会抛出CME。
         * <p>禁止缓存返回Set；禁止外部做check‑then‑act复合操作。
         */
        @Override
        public Set<String> subscribers() {
            return Collections.unmodifiableSet(subscribers);
        }

    }

}