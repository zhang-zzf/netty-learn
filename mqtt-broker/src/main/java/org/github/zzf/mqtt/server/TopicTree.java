package org.github.zzf.mqtt.server;


import static java.util.concurrent.CompletableFuture.runAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;

/**
 * 设计思路：
 * 1. 写操作单线程串行更改
 * 1. 多线程无锁读
 */
public class TopicTree<T> {

    final String threadName;

    protected final ExecutorService executor;

    public TopicTree(String threadName) {
        this.threadName = threadName;
        this.executor = new ThreadPoolExecutor(1, 1,
            60, TimeUnit.SECONDS,
            // 使用无界队列
            new LinkedBlockingDeque<>(),
            (r) -> new Thread(r, threadName),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }


    static String LEVEL_SEPARATOR = "/";
    static String MULTI_LEVEL_WILDCARD = "#";
    static String SINGLE_LEVEL_WILDCARD = "+";

    // tree root
    protected final Node<T> root = new Node<>("*");

    public CompletableFuture<Void> add(String topicFilter, Consumer<AtomicReference<T>> dataOp) {
        return runAsync(() -> doAdd(topicFilter, dataOp), executor);
    }

    private void doAdd(String topicFilter, Consumer<AtomicReference<T>> dataOp) {
        // todo "//"  "/...." ".../"
        String[] topicLevels = topicFilter.split(LEVEL_SEPARATOR);
        Node<T> cur = root;
        for (int i = 0; i < topicLevels.length; i++) {
            String level = topicLevels[i];
            cur = cur.addChild(new Node<>(level));
            if (lastLevel(i, topicLevels)) {
                dataOp.accept(cur.data);
            }
        }
    }

    public CompletableFuture<Void> del(String topicFilter, Consumer<AtomicReference<T>> dataOp) {
        return runAsync(() -> doDel(topicFilter, dataOp), executor);
    }

    private void doDel(String topicFilter, Consumer<AtomicReference<T>> dataOp) {
        String[] topicLevels = topicFilter.split(LEVEL_SEPARATOR);
        dfsRemoveTopic(topicLevels, 0, root, dataOp);
    }

    private void dfsRemoveTopic(String[] topicLevels, int levelIdx, Node<T> parent, Consumer<AtomicReference<T>> dataOp) {
        if (levelIdx >= topicLevels.length) {
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        Node<T> n = parent.childNodes.get(topicLevel);
        if (n == null) {
            return;
        }
        if (lastLevel(levelIdx, topicLevels)) {
            dataOp.accept(n.data);
        }
        else {
            dfsRemoveTopic(topicLevels, levelIdx + 1, n, dataOp);
        }
        // try clean child node if needed.
        if (n.deletable()) {
            parent.childNodes.remove(n.level, n);
        }
    }

    public List<T> match(String topicName) {
        List<T> ret = new ArrayList<>(2);
        dfsMatch(topicName.split(LEVEL_SEPARATOR), 0, root, ret);
        return ret;
    }

    public Optional<T> data(String topicFilter) {
        Node<T> cur = root;
        for (String l : topicFilter.split(LEVEL_SEPARATOR)) {
            cur = cur.childNodes.get(l);
            if (cur == null) {
                break;
            }
        }
        return Optional.ofNullable(cur).map(n -> n.data.get());
    }

    private void dfsMatch(String[] topicLevels, int levelIdx, Node<T> parent, List<T> ret) {
        Node<T> n;
        if (levelIdx == topicLevels.length) {
            if ((n = parent.childNodes.get(MULTI_LEVEL_WILDCARD)) != null) {
                addNode(ret, n);
            }
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        if ((n = parent.childNodes.get(topicLevel)) != null) {
            if (lastLevel(levelIdx, topicLevels)) {
                addNode(ret, n);
            }
            dfsMatch(topicLevels, levelIdx + 1, n, ret);
        }
        if ((n = parent.childNodes.get(MULTI_LEVEL_WILDCARD)) != null) {
            addNode(ret, n);
        }
        if ((n = parent.childNodes.get(SINGLE_LEVEL_WILDCARD)) != null) {
            if (lastLevel(levelIdx, topicLevels)) {
                addNode(ret, n);
            }
            dfsMatch(topicLevels, levelIdx + 1, n, ret);
        }
    }

    private void addNode(List<T> ret, Node<T> n) {
        T data = n.data.get();
        if (data != null) {
            ret.add(data);
        }
    }

    static boolean lastLevel(int level, String[] levelArray) {
        return level == levelArray.length - 1;
    }


    public static class Node<T> {

        final String level;
        final AtomicReference<T> data = new AtomicReference<>();
        /* child Nodes */
        final ConcurrentMap<String, Node<T>> childNodes
            = new ConcurrentHashMap<>(Integer.getInteger("TopicTree.Node.default.childNodes", 4));

        public Node(String level) {
            this.level = level;
        }

        public Node<T> addChild(Node<T> child) {
            Node<T> nextNode;
            if ((nextNode = childNodes.putIfAbsent(child.level, child)) == null) {
                nextNode = child;
            }
            return nextNode;
        }

        public boolean deletable() {
            return childNodes.isEmpty() && data.get() == null;
        }

    }

}
