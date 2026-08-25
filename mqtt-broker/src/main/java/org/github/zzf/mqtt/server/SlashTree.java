package org.github.zzf.mqtt.server;


import static java.util.concurrent.CompletableFuture.runAsync;

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
import lombok.extern.slf4j.Slf4j;

/**
 * 设计思路： 1. 写操作单线程串行更改 1. 多线程无锁读
 */
@Slf4j
public abstract class SlashTree<T> implements AutoCloseable {

    final String threadName;

    protected final ExecutorService executor;

    public SlashTree(String threadName) {
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

    // tree root
    protected final Node<T> root = new Node<>("*");

    public abstract List<T> match(String path);

    public CompletableFuture<Void> add(String path,
            Consumer<AtomicReference<T>> dataOp) {
        return runAsync(() -> doAdd(path, dataOp), executor);
    }

    public CompletableFuture<Void> del(String path,
            Consumer<AtomicReference<T>> dataOp) {
        return runAsync(() -> doDel(path, dataOp), executor);
    }

    public Optional<T> data(String path) {
        Node<T> cur = root;
        for (String l : path.split(LEVEL_SEPARATOR)) {
            cur = cur.childNodes.get(l);
            if (cur == null) {
                break;
            }
        }
        return Optional.ofNullable(cur).map(n -> n.data.get());
    }

    private void doAdd(String path,
            Consumer<AtomicReference<T>> dataOp) {
        String[] levels = path.split(LEVEL_SEPARATOR);
        Node<T> n = root;
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            // n will point to the child after add
            n = n.addChild(new Node<>(level));
            if (lastLevel(i, levels)) {
                n.path = path;
                dataOp.accept(n.data);
            }
        }
    }

    private void doDel(String path,
            Consumer<AtomicReference<T>> dataOp) {
        String[] levels = path.split(LEVEL_SEPARATOR);
        dfsDel(levels, 0, root, dataOp);
    }

    private void dfsDel(String[] levels,
            int levelIdx,
            Node<T> node,
            Consumer<AtomicReference<T>> dataOp) {
        if (levelIdx >= levels.length) {
            node.path = null;
            dataOp.accept(node.data);
            return;
        }
        String level = levels[levelIdx];
        Node<T> n = node.childNodes.get(level);
        if (n == null) {
            return;
        }
        dfsDel(levels, levelIdx + 1, n, dataOp);
        // try clean child node if needed.
        if (n.deletable()) {
            node.childNodes.remove(n.level, n);
        }
    }

    static boolean lastLevel(int level,
            String[] levelArray) {
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
                        log.error("SlashTree({}) executor did not terminate", threadName);
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
        volatile String path;
        final AtomicReference<T> data = new AtomicReference<>();
        /* child Nodes */
        final ConcurrentMap<String, Node<T>> childNodes
                = new ConcurrentHashMap<>(Integer.getInteger("Tree.Node.default.childNodes", 4));

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
