package org.github.zzf.mqtt.server;


import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static org.github.zzf.mqtt.protocol.model.Subscribe.Subscription.V50.fullTopicFilter;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription.V50;
import org.github.zzf.mqtt.protocol.server.RoutingTable;
import org.github.zzf.mqtt.protocol.server.Topic;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date 2026-08-26
 */
public class SharedRoutingTable implements RoutingTable {

    // todo metric 监控订阅的数量和统计信息
    final TopicTree tree = new TopicTree("SharedRoutingTable");

    @Override
    public CompletableFuture<Void> subscribe(String clientId,
            Collection<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(subscriptions.stream()
                .map(s -> this.subscribe(clientId, ((V50) s)))
                .toArray(CompletableFuture[]::new)
        );
    }

    private CompletableFuture<Void> subscribe(String clientId,
            Subscription.V50 sub) {
        String tf = sub.sharedFilter();
        Consumer<AtomicReference<Topic>> dataOp = (AtomicReference<Topic> data) -> {
            SharedTopic topic = (SharedTopic) data.updateAndGet(t -> {
                if (t == null) {
                    return new SharedTopic(tf);
                }
                return t;
            });
            topic.group2Subscribers.computeIfAbsent(sub.sharedGroup(),
                            group -> ConcurrentHashMap.newKeySet(4))
                    .add(clientId);
        };
        // watch out: use filter as path
        return tree.add(tf, dataOp);
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String clientId,
            Collection<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(subscriptions.stream()
                .map(s -> this.unsubscribe(clientId, (V50) s))
                .toArray(CompletableFuture[]::new)
        );
    }

    private CompletableFuture<Void> unsubscribe(String clientId,
            Subscription.V50 sub) {
        String group = sub.sharedGroup();
        return tree.del(sub.sharedFilter(), (AtomicReference<Topic> data) -> {
            SharedTopic topic = (SharedTopic) data.get();
            if (topic != null) {
                Set<String> groupSubscribers = topic.group2Subscribers.get(group);
                groupSubscribers.remove(clientId);
                if (groupSubscribers.isEmpty()) {
                    // clear the group when there is no subscriber
                    topic.group2Subscribers.remove(group);
                }
                if (topic.group2Subscribers.isEmpty()) {
                    // clear the data when there is no group
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

    public static class SharedTopic implements Topic.SharedTopic {
        final String tf;

        final ConcurrentMap<String, Set<String>> group2Subscribers = new ConcurrentHashMap<>();

        public SharedTopic(String tf) {
            this.tf = tf;
        }

        public void add(String clientId, V50 shared) {
            if (!shared.isShared()) {
                return;
            }
            group2Subscribers.computeIfAbsent(shared.sharedGroup(),
                            group -> ConcurrentHashMap.newKeySet(4))
                    .add(clientId);
        }

        @Override
        public Set<Shared> groups() {
            return group2Subscribers.entrySet().stream()
                    .map(e -> new Shared() {
                        @Override
                        public String topicFilter() {
                            return fullTopicFilter(group(), tf);
                        }

                        @Override
                        public String group() {
                            return e.getKey();
                        }

                        @Override
                        public Set<String> subscribers() {
                            return unmodifiableSet(e.getValue());
                        }
                    })
                    .collect(toSet());
        }

        @Override
        public String topicFilter() {
            return tf;
        }

        @Override
        public Set<String> subscribers() {
            throw new UnsupportedOperationException();
        }
    }

}