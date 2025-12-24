package org.github.zzf.mqtt.server;

import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import org.github.zzf.mqtt.protocol.session.server.Topic;
import org.github.zzf.mqtt.protocol.session.server.TopicBlocker;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2025-12-23
 */
public class TopicBlockerImpl implements TopicBlocker {

    final TopicTree<String> tree = new TopicTree<>("TopicBlocker");

    public static final TopicBlocker DEFAULT = new TopicBlockerImpl() {{
        String tfConfig = System.getProperty("mqtt.server.block.tf", "+/server/#");
        add(tfConfig.split(","));
    }};

    @Override
    public Topic match(String topicName) {
        List<String> match = tree.match(topicName);
        return match.isEmpty() ? null : new TopicImpl(match.get(0));
    }

    @Override
    public CompletableFuture<Void> add(String... tfs) {
        if (tfs.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(Arrays.stream(tfs)
            .map(d -> tree.add(d, (AtomicReference<String> data) -> data.set(d)))
            .toArray(CompletableFuture[]::new)
        );
    }

    @Override
    public CompletableFuture<Void> del(String... tfs) {
        if (tfs.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(Arrays.stream(tfs)
            .map(d -> tree.del(d, (AtomicReference<String> data) -> data.set(null)))
            .toArray(CompletableFuture[]::new)
        );
    }

    @RequiredArgsConstructor
    private static class TopicImpl implements Topic {

        final String tf;

        @Override
        public String topicFilter() {
            return tf;
        }

        @Override
        public List<Subscriber> subscribers() {
            return Collections.emptyList();
        }

    }

}