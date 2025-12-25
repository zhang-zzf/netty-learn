package org.github.zzf.mqtt.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import static org.assertj.core.api.BDDAssertions.then;

class TopicTreeTest {

    /**
     * topicName / topicFilter 匹配测试
     */
    @ParameterizedTest(name = "{0} match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenMatch(String topicName, String topicFilter) {
        try (TopicTree<String> tree = new TopicTree<>("TopicTreeTest")) {
            tree.add(topicFilter, ref -> ref.set(topicFilter)).join();
            then(tree.match(topicName)).isNotEmpty().contains(topicFilter);
            tree.del(topicFilter, ref -> ref.set(null)).join();
        }
    }

    /**
     * topicName / topicFilter 不匹配测试
     */
    @ParameterizedTest(name = "{0} will not match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_not_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenNotMatch(String topicName, String topicFilter) {
        try (TopicTree<String> tree = new TopicTree<>("TopicTreeTest")) {
            tree.add(topicFilter, ref -> ref.set(topicFilter)).join();
            then(tree.match(topicName)).isEmpty();
            tree.del(topicFilter, ref -> ref.set(null)).join();
        }
    }

    @Test
    void givenEmpty_whenTopic_then() {
        try (TopicTree<String> tree = new TopicTree<>("TopicTreeTest")) {
            then(tree.match("topic/abc")).isEmpty();
        }
    }

    @ParameterizedTest
    @CsvFileSource(resources = {"/broker/topic_filter.csv"})
    void givenNotEmpty_whenTopic_then(String topicFilter) {
        try (TopicTree<String> tree = new TopicTree<>("TopicTreeTest")) {
            tree.add(topicFilter, ref -> ref.set(topicFilter)).join();
            then(tree.data(topicFilter)).isNotNull().get().isEqualTo(topicFilter);
        }
    }

    /**
     * <p>Broker has Topic 'topic/abc/#' </p>
     * <p>'topic/abc' will not match it</p>
     * <p>'topic/abc/' will not match it</p>
     * <p>'topic/abc/+' will not match it</p>
     */
    @Test
    void givenNotEmpty_whenTopicNotExist_thenEmpty() {
        String topicFilter = "topic/abc/#";
        try (TopicTree<String> tree = new TopicTree<>("TopicTreeTest")) {
            tree.add(topicFilter, ref -> ref.set(topicFilter)).join();
            then(tree.data(topicFilter)).isNotEmpty().get().isEqualTo(topicFilter);
            then(tree.data("topic/abc")).isEmpty();
            then(tree.data("topic/abc/")).isEmpty();
            then(tree.data("topic/abc/+")).isEmpty();
        }
    }

    @Test
    void givenBroker_whenSubscribeAndUnsubscribe_then() {
        String topicFilter = "topic/abc/#";
        try (TopicTree<String> tree = new TopicTree<>("TopicTreeTest")) {
            tree.add(topicFilter, ref -> ref.set(topicFilter)).join();
            then(tree.data(topicFilter)).isNotEmpty().get().isEqualTo(topicFilter);
            tree.del(topicFilter, ref -> ref.set(null)).join();
            then(tree.data(topicFilter)).isEmpty();
        }
    }

    @Test
    void givenBroker_whenAddThenDel_then() {
        String topicFilter = "topic/abc/#";
        try (TopicTree<String> tree = new TopicTree<>("TopicTreeTest")) {
            tree.add("topic", ref -> ref.set("topic")).join();
            tree.add(topicFilter, ref -> ref.set(topicFilter)).join();
            then(tree.data(topicFilter)).isNotEmpty().get().isEqualTo(topicFilter);
            tree.del("topic/abc", ref -> ref.set(null)).join();
            tree.del("topic", ref -> ref.set(null)).join();
            then(tree.data(topicFilter)).isNotEmpty();
        }
    }

}