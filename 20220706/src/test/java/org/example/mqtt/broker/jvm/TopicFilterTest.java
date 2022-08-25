package org.example.mqtt.broker.jvm;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.TopicFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/15
 */
@Slf4j
class TopicFilterTest {

    TopicFilter tf;

    @BeforeEach
    public void beforeEach() {
        tf = new SingleThreadTopicFilter();
    }

    /**
     * 不包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddPrecise_thenSuccess() {
        String topicFilter = "a/b";
        tf.add(topicFilter);
        Set<String> match = tf.match(topicFilter);
        then(match).contains(topicFilter);
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>TopicFilter: a/b/#; Topic: a/b -> not Match </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter2_thenMatch() {
        String topicFilter = "a/b/#";
        tf.add(topicFilter);
        Set<String> match = tf.match("a/b");
        then(match).contains(topicFilter);
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>TopicFilter 非法测试</p>
     * <p>TopicFilter: a/b/#/b -> IllegalArgumentException </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddIllegalTopicFilter_thenThrowException() {
        Throwable throwable = catchThrowable(() -> tf.add("a/b/#/c"));
        then(throwable).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>TopicFilter: /a/#</p>
     * <p>Topic: /a/b -> Match </p>
     * <p>Topic: a/b -> not Match </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter3_thenMatchSuccess() {
        String topicFilter = "/a/#";
        tf.add(topicFilter);
        then(tf.match("/a/b")).contains(topicFilter);
        then(tf.match("a/b")).isEmpty();
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>TopicFilter: /a/#</p>
     * <p>Topic: /a/b -> Match </p>
     * <p>Topic: a/b -> not Match </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter4_thenMatchSuccess() {
        String topicFilter = "/a/#";
        tf.add(topicFilter);
        then(tf.match("/a/b")).contains(topicFilter);
        then(tf.match("a/b")).isEmpty();
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>add TopicFilter: a/b/#</p>
     * <p>topic: a/c -> not match </p>
     * <p>add TopicFilter: a/+</p>
     * <p>topic: a/b -> match </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter5_thenMatchSuccess() {
        tf.add("a/b/#");
        then(tf.match("a/c")).isEmpty();
        tf.add("a/+");
        then(tf.match("a/b")).contains("a/+");
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>逆向测试流程：测试接口语义的正确性</p>
     * <p>add TopicFilter: a/#</p>
     * <p>remove TopicFilter: a/#</p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter10_thenMatchSuccess() throws InterruptedException {
        tf.add("a/#");
        tf.remove("a/#");
        then(tf.match("a/b")).contains();
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>逆向测试流程：测试接口语义的正确性</p>
     * <p>add TopicFilter: a/b/#</p>
     * <p>remove TopicFilter: a/b/+</p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter11_thenMatchSuccess() throws InterruptedException {
        tf.add("a/b/#");
        tf.remove("a/b/+");
        then(tf.match("a/b/c")).contains("a/b/#");
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>逆向测试流程：测试接口语义的正确性</p>
     * <p>add TopicFilter: a/b/#</p>
     * <p>remove TopicFilter: a/#</p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter12_thenMatchSuccess() throws InterruptedException {
        tf.add("a/b/#");
        tf.remove("a/#");
        then(tf.match("a/b/c")).contains("a/b/#");
    }

    /**
     * TopicFilter 1000W 压力测试
     * <p>分析内存占用</p>
     * <pre>
     *     JVM稳定后占用约 9.5G 内存
     *     - 6G -> 2700W char[] 数组对象 约等于 237 b/字符串 （32*7=224）
     *     - 600M -> 2700W String 对象（每个 String 对象占用 24 个字节）
     *     - 3G -> 1600W TopicFilter.Node (约 400M)
     *          -> 1600W ConcurrentHashMap (约 1G)
     *          -> 1600W ConcurrentHashMap.Node (约500M)
     * </pre>
     * <p>单线程7层分析查询效率</p>
     * <pre>
     *     now tf has 10,761,678 topicFilter 1000W
     *     test 10000000 cnt, use time: 29419322611ns, 2941ns/per
     *     QPS = 333,000 约 33W/秒的匹配查询
     * </pre>
     */
    @Test
    @Disabled
    void givenTopicFilter_whenAdd10000000_when() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        Runnable task = () -> {
            int totalLevel = 6, cntPerLevel = 9;
            int total = 0;
            List<Set<String>> topic = new ArrayList<>(totalLevel);
            for (int i = 0; i < totalLevel; i++) {
                Set<String> list = new HashSet<>(10);
                if (i == 0) {
                    for (int j = 0; j < cntPerLevel; j++) {
                        list.add(UUID.randomUUID().toString());
                    }
                } else {
                    Set<String> level = topic.get(i - 1);
                    for (String l : level) {
                        for (int j = 0; j < cntPerLevel; j++) {
                            list.add(l + "/" + UUID.randomUUID().toString());
                        }
                    }
                }
                log.info("curLevel: {} -> topic: {}", i, list.size());
                topic.add(list);
                for (String s : list) {
                    tf.add(s + "/+");
                    tf.add(s + "/#");
                    total += 2;
                }
                log.info("now tf has {} topicFilter", total);
            }
            // test ut
            long nanoTime = System.nanoTime();
            int totalTestCnt = 0;
            while (latch.getCount() != 0) {
                for (int i = 0; i < topic.size(); i++) {
                    for (String s : topic.get(i)) {
                        tf.match(s);
                        totalTestCnt += 1;
                        if (totalTestCnt == Long.MAX_VALUE - 1) {
                            long useTime = System.nanoTime() - nanoTime;
                            log.info("test {} cnt, use time: {}ns, {}ns/per", totalTestCnt, useTime, useTime / totalTestCnt);
                            totalTestCnt = 0;
                            nanoTime = System.nanoTime();
                        }
                    }
                }
            }
            long useTime = System.nanoTime() - nanoTime;
            log.info("test {} cnt, use time: {}ns, {}ns/per", totalTestCnt, useTime, useTime / totalTestCnt);
            // test ut
        };
        new Thread(task).start();
        Thread.sleep(300000);
        latch.countDown();
        Thread.currentThread().join();
    }

    /**
     * TopicFilter 1000W 压力测试
     * <p>分析内存占用</p>
     * <pre>
     *     JVM稳定后占用约 9.5G 内存
     *     - 6G -> 2700W char[] 数组对象 约等于 237 b/字符串 （32*7=224）
     *     - 600M -> 2700W String 对象（每个 String 对象占用 24 个字节）
     *     - 3G -> 1600W TopicFilter.Node (约 400M)
     *          -> 1600W ConcurrentHashMap (约 1G)
     *          -> 1600W ConcurrentHashMap.Node (约500M)
     * </pre>
     * <p>多线程7层分析查询效率</p>
     * <pre>
     *     now tf has 10,761,678 topicFilter 1000W
     *     test 2834307888 cnt, use time: 300008541983ns, 105ns/per
     *     test 530227043 cnt, use time: 300008434183ns, 565ns/per
     *     test 350353231 cnt, use time: 300008148483ns, 856ns/per
     *     test 204356240 cnt, use time: 300007558331ns, 1468ns/per
     *     test 146508429 cnt, use time: 300008686164ns, 2047ns/per
     *     test 106183582 cnt, use time: 300007243579ns, 2825ns/per
     *     test 75040733 cnt, use time: 300007885698ns, 3997ns/per
     *     300s内共执行了 4246977146 次查询，QPS：14,156,590 约 1400W/s
     * </pre>
     */
    @Test
    // @Disabled
    void givenTopicFilter_whenAdd10000000_whenTestMultiThreadRead() throws InterruptedException {
        int totalLevel = 6, cntPerLevel = 9;
        int total = 0;
        List<Set<String>> topic = new ArrayList<>(totalLevel);
        for (int i = 0; i < totalLevel; i++) {
            Set<String> list = new HashSet<>(10);
            if (i == 0) {
                for (int j = 0; j < cntPerLevel; j++) {
                    list.add(UUID.randomUUID().toString());
                }
            } else {
                Set<String> level = topic.get(i - 1);
                for (String l : level) {
                    for (int j = 0; j < cntPerLevel; j++) {
                        list.add(l + "/" + UUID.randomUUID().toString());
                    }
                }
            }
            log.info("curLevel: {} -> topic: {}", i, list.size());
            topic.add(list);
            for (String s : list) {
                tf.add(s + "/+");
                tf.add(s + "/#");
                total += 2;
            }
            log.info("now tf has {} topicFilter", total);
        }
        // test ut
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < totalLevel * 2; i++) {
            final Set<String> level = topic.get(i % totalLevel);
            new Thread(() -> {
                long nanoTime = System.nanoTime();
                long totalTestCnt = 0;
                while (latch.getCount() != 0) {
                    // test ut
                    for (String s : level) {
                        totalTestCnt += 1;
                        tf.match(s);
                        if (latch.getCount() == 0) {
                            break;
                        }
                        if (totalTestCnt == Long.MAX_VALUE - 1) {
                            long useTime = System.nanoTime() - nanoTime;
                            log.info("test {} cnt, use time: {}ns, {}ns/per", totalTestCnt, useTime, useTime / totalTestCnt);
                            totalTestCnt = 0;
                            nanoTime = System.nanoTime();
                        }
                    }
                }
                long useTime = System.nanoTime() - nanoTime;
                log.info("test {} cnt, use time: {}ns, {}ns/per", totalTestCnt, useTime, useTime / totalTestCnt);
            }).start();
        }
        Thread.sleep(300000);
        latch.countDown();
        Thread.currentThread().join();
    }

}
