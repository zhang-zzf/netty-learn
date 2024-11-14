package org.example.mqtt.broker.node;

import static org.assertj.core.api.BDDAssertions.then;

import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Subscribe;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

class DefaultBrokerStateTest {

    /**
     * topicName / topicFilter 匹配测试
     */
    @ParameterizedTest(name = "{0} match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenMatch(String topicName, String topicFilter) {
        then(DefaultBrokerState.topicNameMatchTopicFilter(topicName, topicFilter)).isTrue();
    }

    /**
     * topicName / topicFilter 不匹配测试
     */
    @ParameterizedTest(name = "{0} will not match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_not_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenNotMatch(String topicName, String topicFilter) {
        then(DefaultBrokerState.topicNameMatchTopicFilter(topicName, topicFilter)).isFalse();
    }

    @Test
    void givenEmpty_whenTopic_then() {
        DefaultBrokerState brokerState = new DefaultBrokerState();
        Optional<Topic> topic = brokerState.topic("/topic/abc");
        then(topic).isEmpty();
    }

    @ParameterizedTest
    @CsvFileSource(resources = {"/broker/topic_filter.csv"})
    void givenNotEmpty_whenTopic_then(String topicFilter) throws ExecutionException, InterruptedException {
        DefaultBrokerState brokerState = new DefaultBrokerState();
        Subscribe.Subscription subscription = new Subscribe.Subscription(topicFilter, 2);
        brokerState.subscribe(buildServerSession(), subscription).get();
        Optional<Topic> topic = brokerState.topic(topicFilter);
        then(topic).isNotEmpty().get().returns(topicFilter, Topic::topicFilter);
    }

    @NotNull
    private static DefaultServerSession buildServerSession() {
        return new DefaultServerSession(Connect.from("c1", (short) 1), new EmbeddedChannel(), null);
    }

    /**
     * <p>Broker has Topic 'topic/abc/#' </p>
     * <p>'topic/abc' will not match it</p>
     * <p>'topic/abc/' will not match it</p>
     * <p>'topic/abc/+' will not match it</p>
     */
    @Test
    void givenNotEmpty_whenTopicNotExist_thenEmpty() throws ExecutionException, InterruptedException {
        DefaultBrokerState brokerState = new DefaultBrokerState();
        Subscribe.Subscription subscription = new Subscribe.Subscription("topic/abc/#", 2);
        brokerState.subscribe(buildServerSession(), subscription).get();
        then(brokerState.topic("topic/abc")).isEmpty();
        then(brokerState.topic("topic/abc/")).isEmpty();
        then(brokerState.topic("topic/abc/+")).isEmpty();
    }

    /**
     * 订阅 -> 取消订阅
     */
    @Test
    void givenBroker_whenSubscribeAndUnsubscribe_then() throws ExecutionException, InterruptedException {
        String topicFilter = "topic/abc/#";
        DefaultBrokerState brokerState = new DefaultBrokerState();
        Subscribe.Subscription subscription = new Subscribe.Subscription(topicFilter, 2);
        DefaultServerSession session = buildServerSession();
        brokerState.subscribe(session, subscription).get();
        then(brokerState.topic(topicFilter)).isNotEmpty();
        brokerState.unsubscribe(session, subscription).get();
        then(brokerState.topic(topicFilter)).isEmpty();
    }

}