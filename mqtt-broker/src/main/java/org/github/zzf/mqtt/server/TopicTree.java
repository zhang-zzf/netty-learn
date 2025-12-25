package org.github.zzf.mqtt.server;


import static java.util.concurrent.CompletableFuture.runAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * 设计思路： 1. 写操作单线程串行更改 1. 多线程无锁读
 */
@Slf4j
public class TopicTree<T> implements AutoCloseable {

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

    static final String LEVEL_SEPARATOR = "/";
    static final String MULTI_LEVEL_WILDCARD = "#";
    static final String SINGLE_LEVEL_WILDCARD = "+";
    static final String $ = "$";

    // tree root
    protected final Node<T> root = new Node<>("*");

    public CompletableFuture<Void> add(String topicFilter, Consumer<AtomicReference<T>> dataOp) {
        return runAsync(() -> doAdd(topicFilter, dataOp), executor);
    }

    private void doAdd(String topicFilter, Consumer<AtomicReference<T>> dataOp) {
        // todo "//"  "/...." ".../"
        String[] topicLevels = topicFilter.split(LEVEL_SEPARATOR);
        Node<T> n = root;
        for (int i = 0; i < topicLevels.length; i++) {
            String level = topicLevels[i];
            // n will point to the child after add
            n = n.addChild(new Node<>(level));
            if (lastLevel(i, topicLevels)) {
                n.topicFilter = topicFilter;
                dataOp.accept(n.data);
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

    private void dfsRemoveTopic(String[] topicLevels, int levelIdx, Node<T> node, Consumer<AtomicReference<T>> dataOp) {
        if (levelIdx >= topicLevels.length) {
            node.topicFilter = null;
            dataOp.accept(node.data);
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        Node<T> n = node.childNodes.get(topicLevel);
        if (n == null) {
            return;
        }
        dfsRemoveTopic(topicLevels, levelIdx + 1, n, dataOp);
        // try clean child node if needed.
        if (n.deletable()) {
            node.childNodes.remove(n.level, n);
        }
    }

    public List<T> match(String topicName) {
        List<Node<T>> ret = new ArrayList<>(2);
        dfsMatch(topicName.split(LEVEL_SEPARATOR), 0, root, ret);
        Stream<Node<T>> stream;
        // The Server MUST NOT match Topic Filters starting with a wildcard character (# or +) with Topic Names beginning with a $ character
        if (topicName.startsWith($)) {
            stream = ret.stream().filter(this::dollarMatch);
        }
        else {
            stream = ret.stream();
        }
        return stream
            .map(n -> n.data.get())
            .filter(Objects::nonNull)
            .toList()
            ;
    }

    private boolean dollarMatch(Node<T> t) {
        if (t.topicFilter.startsWith(MULTI_LEVEL_WILDCARD)
            || t.topicFilter.startsWith(SINGLE_LEVEL_WILDCARD)) {
            return false;
        }
        return true;
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

    private void dfsMatch(String[] topicLevels, int levelIdx, Node<T> cur, List<Node<T>> ret) {
        Node<T> n;
        if (levelIdx == topicLevels.length) {
            addNode(ret, cur);
            if ((n = cur.childNodes.get(MULTI_LEVEL_WILDCARD)) != null) {
                addNode(ret, n);
            }
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        if ((n = cur.childNodes.get(topicLevel)) != null) {
            dfsMatch(topicLevels, levelIdx + 1, n, ret);
        }
        if ((n = cur.childNodes.get(MULTI_LEVEL_WILDCARD)) != null) {
            addNode(ret, n);
        }
        if ((n = cur.childNodes.get(SINGLE_LEVEL_WILDCARD)) != null) {
            dfsMatch(topicLevels, levelIdx + 1, n, ret);
        }
    }

    private void addNode(List<Node<T>> ret, Node<T> node) {
        if (node.topicFilter != null) {
            ret.add(node);
        }
    }

    static boolean lastLevel(int level, String[] levelArray) {
        return level == levelArray.length - 1;
    }

    @Override
    public void close() {
        if (!executor.isShutdown()) {
            // 1. 阻止新任务提交
            executor.shutdown();
            try {
                // 2. 等待已提交任务完成（设置超时时间）
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    // 3. 强制关闭仍在执行的任务
                    executor.shutdownNow();
                    // 4. 再次等待终止
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.error("TopicTree({}) executor did not terminate", threadName);
                    }
                }
            } catch (InterruptedException e) {
                // 5. 恢复中断状态并强制关闭
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class Node<T> {

        final String level;
        volatile String topicFilter;
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
