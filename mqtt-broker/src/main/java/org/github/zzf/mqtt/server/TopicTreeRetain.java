package org.github.zzf.mqtt.server;


import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.server.RetainPublishManager;

/**
 * 设计思路： 1. 写操作单线程串行更改 1. 多线程无锁读
 */
public class TopicTreeRetain extends TopicTree<Publish> implements RetainPublishManager {

    public static final TopicTreeRetain DEFAULT = new TopicTreeRetain("RetainPublishManager");

    public TopicTreeRetain(String threadName) {
        super(threadName);
    }

    @Override
    public List<Publish> match(String tf) {
        List<Publish> ret = new ArrayList<>();
        dfsMatch(tf.split(LEVEL_SEPARATOR), 0, root, ret, false);
        return postHandle(ret, tf);
    }

    private List<Publish> postHandle(List<Publish> ret, String tf) {
        if (tf.startsWith(MULTI_LEVEL_WILDCARD) || tf.startsWith(SINGLE_LEVEL_WILDCARD)) {
            return ret.stream().filter(d -> !d.topicName().startsWith($)).toList();
        }
        return ret;
    }

    @Override
    public CompletableFuture<List<Publish>> match(String... topicFilters) {
        if (topicFilters.length == 0) {
            return CompletableFuture.completedFuture(emptyList());
        }
        return supplyAsync(() -> {
            List<Publish> ret = new ArrayList<>();
            for (String tf : topicFilters) {
                dfsMatch(tf.split(LEVEL_SEPARATOR), 0, root, ret, false);
                ret = postHandle(ret, tf);
            }
            return ret;
        }, executor);
    }

    private void dfsMatch(String[] topicLevels, int levelIdx, Node<Publish> cur, List<Publish> ret, boolean matched) {
        if (matched) {
            addNode(ret, cur);
            for (Node<Publish> node : cur.childNodes.values()) {
                dfsMatch(topicLevels, levelIdx + 1, node, ret, true);
            }
            return;
        }
        // match first then check levelIdx
        // a/# will match a/b a/b/c and so on
        if (levelIdx == topicLevels.length) {
            addNode(ret, cur);
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        Node<Publish> child = cur.childNodes.get(topicLevel);
        if (child != null) {
            dfsMatch(topicLevels, levelIdx + 1, child, ret, false);
        }
        else if (topicLevel.equals(MULTI_LEVEL_WILDCARD)) {
            addNode(ret, cur); // 'a/#' will match 'a' 'a/b' 'a/b/c' 'a/c'
            for (Node<Publish> node : cur.childNodes.values()) {
                dfsMatch(topicLevels, levelIdx + 1, node, ret, true);
            }
        }
        else if (topicLevel.equals(SINGLE_LEVEL_WILDCARD)) {
            for (Node<Publish> node : cur.childNodes.values()) {
                dfsMatch(topicLevels, levelIdx + 1, node, ret, false);
            }
        }
    }

    private void addNode(List<Publish> ret, Node<Publish> n) {
        Publish data = n.data.get();
        if (data != null) {
            ret.add(data);
        }
    }

    @Override
    public CompletableFuture<Void> add(Publish... packets) {
        if (packets.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(Arrays.stream(packets)
            .map(d -> add(d.topicName(), (AtomicReference<Publish> data) -> data.set(d)))
            .toArray(CompletableFuture[]::new)
        );
    }

    @Override
    public CompletableFuture<Void> del(Publish... packets) {
        if (packets.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(Arrays.stream(packets)
            .map(d -> del(d.topicName(), (AtomicReference<Publish> data) -> data.set(null)))
            .toArray(CompletableFuture[]::new)
        );
    }
}
