package org.example.mqtt.broker.jvm;

import org.example.mqtt.broker.TopicFilter;
import org.junit.jupiter.api.Test;

import java.util.Set;

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



}