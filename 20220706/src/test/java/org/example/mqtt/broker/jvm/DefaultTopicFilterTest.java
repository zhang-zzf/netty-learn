package org.example.mqtt.broker.jvm;

import org.example.mqtt.broker.TopicFilter;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/15
 */
class DefaultTopicFilterTest {

    /**
     * 不包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddPrecise_thenSuccess() {
        TopicFilter tf = new DefaultTopicFilter();
        String topicFilter = "a/b";
        tf.add(topicFilter);
        Set<String> match = tf.match(topicFilter);
        then(match).contains(topicFilter);
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter_thenSuccess() {
        TopicFilter tf = new DefaultTopicFilter();
        String topicFilter = "a/#";
        tf.add(topicFilter);
        Set<String> match = tf.match("a/b");
        then(match).contains(topicFilter);
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>TopicFilter: a/b/#; Topic: a/b -> not Match </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter2_thenMatchFailed() {
        TopicFilter tf = new DefaultTopicFilter();
        String topicFilter = "a/b/#";
        tf.add(topicFilter);
        Set<String> match = tf.match("a/b");
        then(match).isEmpty();
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>TopicFilter 非法测试</p>
     * <p>TopicFilter: a/b/#/b -> IllegalArgumentException </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddIllegalTopicFilter_thenThrowException() {
        TopicFilter tf = new DefaultTopicFilter();
        String topicFilter = "a/b/#/c";
        Throwable throwable = catchThrowable(() -> tf.add(topicFilter));
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
        TopicFilter tf = new DefaultTopicFilter();
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
        TopicFilter tf = new DefaultTopicFilter();
        String topicFilter = "/a/#";
        tf.add(topicFilter);
        then(tf.match("/a/b")).contains(topicFilter);
        then(tf.match("a/b")).isEmpty();
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>add TopicFilter: a/b/#</p>
     * <p>topic: a/b -> not match </p>
     * <p>add TopicFilter: a/+</p>
     * <p>topic: a/b -> match </p>
     */
    @Test
    void givenDefaultTopicFilter_whenAddWildcardsTopicFilter5_thenMatchSuccess() {
        TopicFilter tf = new DefaultTopicFilter();
        tf.add("a/b/#");
        then(tf.match("a/b")).isEmpty();
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
        TopicFilter tf = new DefaultTopicFilter();
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
        TopicFilter tf = new DefaultTopicFilter();
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
        TopicFilter tf = new DefaultTopicFilter();
        tf.add("a/b/#");
        tf.remove("a/#");
        then(tf.match("a/b/c")).contains("a/b/#");
    }

    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>add TopicFilter: a/b/#</p>
     * <p>add TopicFilter: a/+</p>
     * <p>remove TopicFilter: a/+</p>
     * <p>add TopicFilter: a/+</p>
     * <p>Topic: a/b -> match</p>
     */
    @Test
    void givenDefaultTopicFilter_whenRemove10_thenMatchSuccess() throws InterruptedException {
        TopicFilter tf = new DefaultTopicFilter();
        tf.add("a/+");
        Thread t1 = new Thread(() -> tf.remove("a/+"));
        t1.start();
        Thread t2 = new Thread(() -> tf.add("a/+"));
        t2.start();
        // wait
        t1.join();
        t2.join();
    }
    /**
     * 包含 wildcards 的 TopicFilter 流程 UT
     * <p>正向测试流程：测试接口语义的正确性</p>
     * <p>add TopicFilter: a/+</p>
     * <pre>
     *     concurrent:
     *     1. remove a/+
     *     2. add a/+/c
     * </pre>
     * <p>Topic: a/b/c -> match</p>
     */
    @Test
    void givenDefaultTopicFilter_whenRemove11_thenMatchSuccess() throws InterruptedException {
        TopicFilter tf = new DefaultTopicFilter();
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 10000; i++) {
            tf.add("a/+");
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch taskDone = new CountDownLatch(2);
            executorService.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                }
                tf.remove("a/+");
                taskDone.countDown();
            });
            executorService.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                }
                tf.add("a/+/c");
                taskDone.countDown();
            });
            // start the concurrent task
            start.countDown();
            taskDone.await();
            then(tf.match("a/b/c")).contains("a/+/c");
        }
    }


}