package org.github.zzf.mqtt.server;

import static org.assertj.core.api.BDDAssertions.then;

import java.util.Objects;
import java.util.Set;
import org.github.zzf.mqtt.protocol.server.Topic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

class TopicTreeTest {

    /**
     * topicName / topicFilter 匹配测试
     */
    @ParameterizedTest(name = "{0} match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenMatch(String topicName, String topicFilter) {
        try (TopicTree tree = new TopicTree("TopicTreeTest")) {
            TopicImpl topic = new TopicImpl(topicFilter);
            tree.add(topicFilter, ref -> ref.set(topic)).join();
            then(tree.match(topicName)).isNotEmpty().contains(topic);
            tree.del(topicFilter, ref -> ref.set(null)).join();
        }
    }

    /**
     * topicName / topicFilter 不匹配测试
     */
    @ParameterizedTest(name = "{0} will not match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_not_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenNotMatch(String topicName, String topicFilter) {
        try (TopicTree tree = new TopicTree("TopicTreeTest")) {
            TopicImpl topic = new TopicImpl(topicFilter);
            tree.add(topicFilter, ref -> ref.set(topic)).join();
            then(tree.match(topicName)).isEmpty();
            tree.del(topicFilter, ref -> ref.set(null)).join();
        }
    }

    @Test
    void givenEmpty_whenTopic_then() {
        try (TopicTree tree = new TopicTree("TopicTreeTest")) {
            then(tree.match("topic/abc")).isEmpty();
        }
    }

    @ParameterizedTest
    @CsvFileSource(resources = {"/broker/topic_filter.csv"})
    void givenNotEmpty_whenTopic_then(String topicFilter) {
        try (TopicTree tree = new TopicTree("TopicTreeTest")) {
            TopicImpl topic = new TopicImpl(topicFilter);
            tree.add(topicFilter, ref -> ref.set(topic)).join();
            then(tree.data(topicFilter)).isNotNull().get().isEqualTo(topic);
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
        try (TopicTree tree = new TopicTree("TopicTreeTest")) {
            TopicImpl topic = new TopicImpl(topicFilter);
            tree.add(topicFilter, ref -> ref.set(topic)).join();
            then(tree.data(topicFilter)).isNotEmpty().get().isEqualTo(topic);
            then(tree.data("topic/abc")).isEmpty();
            then(tree.data("topic/abc/")).isEmpty();
            then(tree.data("topic/abc/+")).isEmpty();
        }
    }

    @Test
    void givenBroker_whenSubscribeAndUnsubscribe_then() {
        String topicFilter = "topic/abc/#";
        try (TopicTree tree = new TopicTree("TopicTreeTest")) {
            TopicImpl topic = new TopicImpl(topicFilter);
            tree.add(topicFilter, ref -> ref.set(topic)).join();
            then(tree.data(topicFilter)).isNotEmpty().get().isEqualTo(topic);
            tree.del(topicFilter, ref -> ref.set(null)).join();
            then(tree.data(topicFilter)).isEmpty();
        }
    }

    @Test
    void givenBroker_whenAddThenDel_then() {
        String topicFilter = "topic/abc/#";
        try (TopicTree tree = new TopicTree("TopicTreeTest")) {
            tree.add("topic", ref -> ref.set(new TopicImpl("topic"))).join();
            TopicImpl topic = new TopicImpl(topicFilter);
            tree.add(topicFilter, ref -> ref.set(topic)).join();
            then(tree.data(topicFilter)).isNotEmpty().get().isEqualTo(topic);
            tree.del("topic/abc", ref -> ref.set(null)).join();
            tree.del("topic", ref -> ref.set(null)).join();
            then(tree.data(topicFilter)).isNotEmpty();
        }
    }

    record TopicImpl(String tf) implements Topic {

        @Override
        public String topicFilter() {
            return "";
        }

        @Override
        public Set<String> subscribers() {
            return Set.of();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopicImpl topic = (TopicImpl) o;
            return Objects.equals(tf, topic.tf);
        }

    }

}