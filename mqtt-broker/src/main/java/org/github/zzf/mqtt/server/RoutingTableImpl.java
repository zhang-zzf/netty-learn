package org.github.zzf.mqtt.server;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.session.server.RoutingTable;
import org.github.zzf.mqtt.protocol.session.server.Topic;
import org.github.zzf.mqtt.protocol.session.server.Topic.Subscriber;

// todo UT
/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2025-12-21
 */
public class RoutingTableImpl implements RoutingTable {

    // todo metric 监控订阅的数量和统计信息
    final TopicTree<Topic> tree = new TopicTree<>("RoutingTable");

    @Override
    public CompletableFuture<Void> subscribe(String clientId, Collection<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(subscriptions.stream()
            .map(s -> this.subscribe(clientId, s))
            .toArray(CompletableFuture[]::new)
        );
    }

    private CompletableFuture<Void> subscribe(String clientId, Subscription subscription) {
        Consumer<AtomicReference<Topic>> dataOp = (AtomicReference<Topic> data) -> {
            TopicImpl topic = (TopicImpl) data.updateAndGet(t -> {
                if (t == null) {
                    return new TopicImpl(subscription.topicFilter());
                }
                return t;
            });
            // 强制覆盖 qos
            topic.subscribers.put(clientId, new SubscriberImpl(clientId, subscription.qos()));
        };
        return tree.add(subscription.topicFilter(), dataOp);
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String clientId, Collection<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(subscriptions.stream()
            .map(s -> this.unsubscribe(clientId, s))
            .toArray(CompletableFuture[]::new)
        );
    }

    private CompletableFuture<Void> unsubscribe(String clientId, Subscription subscription) {
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

    /**
     * 线程安全
     */
    @RequiredArgsConstructor
    private static class TopicImpl implements Topic {

        final String tf;
        final ConcurrentMap<String, Subscriber> subscribers
            = new ConcurrentHashMap<>(Integer.getInteger("TopicImpl.subscribers.default.size", 4));

        @Override
        public String topicFilter() {
            return tf;
        }

        @Override
        public List<Subscriber> subscribers() {
            // 使用 Topic 的视图。在迭代时，若发生修改，结果不可知
            // return unmodifiableCollection(subscribers.values()).iterator();
            return new ArrayList<>(subscribers.values());
        }

    }

    public record SubscriberImpl(String clientId, int qos) implements Subscriber {
    }

}