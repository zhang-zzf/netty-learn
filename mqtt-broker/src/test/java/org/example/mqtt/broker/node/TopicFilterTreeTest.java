package org.example.mqtt.broker.node;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.util.HashSet;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.BDDAssertions.then;

class TopicFilterTreeTest {

    /**
     * topicName / topicFilter 匹配测试
     */
    @ParameterizedTest(name = "{0} match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenMatch(String topicName, String topicFilter) {
        then(DefaultBroker.TopicFilterTree.from(new HashSet<>(singletonList(topicFilter))).match(topicName)).isNotEmpty();
    }

    /**
     * topicName / topicFilter 不匹配测试
     */
    @ParameterizedTest(name = "{0} will not match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_not_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenNotMatch(String topicName, String topicFilter) {
        then(DefaultBroker.TopicFilterTree.from(new HashSet<>(singletonList(topicFilter))).match(topicName)).isEmpty();
    }

}