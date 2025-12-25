package org.github.zzf.mqtt.server;

import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.server.Topic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;

class RoutingTableImplTest {

    /**
     * topicName / topicFilter 匹配测试
     */
    @ParameterizedTest(name = "{0} match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenMatch(String topicName, String topicFilter) {
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        Subscription subscription = new Subscription(topicFilter, 1);

        routingTable.subscribe(clientId, Arrays.asList(subscription)).join();
        then(routingTable.match(topicName)).isNotEmpty();
        then(routingTable.match(topicName).get(0).topicFilter()).isEqualTo(topicFilter);

        // 清理
        routingTable.unsubscribe(clientId, Arrays.asList(subscription)).join();
    }

    /**
     * topicName / topicFilter 不匹配测试
     */
    @ParameterizedTest(name = "{0} will not match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_not_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenNotMatch(String topicName, String topicFilter) {
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        Subscription subscription = new Subscription(topicFilter, 1);

        routingTable.subscribe(clientId, Arrays.asList(subscription)).join();
        then(routingTable.match(topicName)).isEmpty();

        // 清理
        routingTable.unsubscribe(clientId, Arrays.asList(subscription)).join();
    }

    @Test
    void givenEmpty_whenTopic_then() {
        RoutingTableImpl routingTable = new RoutingTableImpl();
        then(routingTable.match("topic/abc")).isEmpty();
    }

    @ParameterizedTest
    @CsvFileSource(resources = {"/broker/topic_filter.csv"})
    void givenNotEmpty_whenTopic_then(String topicFilter) {
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        Subscription subscription = new Subscription(topicFilter, 1);

        routingTable.subscribe(clientId, Arrays.asList(subscription)).join();
        then(routingTable.topic(topicFilter)).isNotEmpty();
        then(routingTable.topic(topicFilter).get().topicFilter()).isEqualTo(topicFilter);
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
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        Subscription subscription = new Subscription(topicFilter, 1);

        routingTable.subscribe(clientId, Arrays.asList(subscription)).join();
        then(routingTable.topic(topicFilter)).isNotEmpty();
        then(routingTable.topic(topicFilter).get().topicFilter()).isEqualTo(topicFilter);
        then(routingTable.topic("topic/abc")).isEmpty();
        then(routingTable.topic("topic/abc/")).isEmpty();
        then(routingTable.topic("topic/abc/+")).isEmpty();
    }

    @Test
    void givenBroker_whenSubscribeAndUnsubscribe_then() {
        String topicFilter = "topic/abc/#";
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        Subscription subscription = new Subscription(topicFilter, 1);

        routingTable.subscribe(clientId, Arrays.asList(subscription)).join();
        then(routingTable.topic(topicFilter)).isNotEmpty();
        then(routingTable.topic(topicFilter).get().topicFilter()).isEqualTo(topicFilter);

        routingTable.unsubscribe(clientId, Arrays.asList(subscription)).join();
        then(routingTable.topic(topicFilter)).isEmpty();
    }

    @Test
    void givenBroker_whenAddThenDel_then() {
        String topicFilter1 = "topic";
        String topicFilter2 = "topic/abc/#";
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        Subscription subscription1 = new Subscription(topicFilter1, 1);
        Subscription subscription2 = new Subscription(topicFilter2, 1);

        routingTable.subscribe(clientId, Arrays.asList(subscription1)).join();
        routingTable.subscribe(clientId, Arrays.asList(subscription2)).join();
        then(routingTable.topic(topicFilter2)).isNotEmpty();
        then(routingTable.topic(topicFilter2).get().topicFilter()).isEqualTo(topicFilter2);

        // 取消订阅一个不存在的主题，应该不影响其他主题
        Subscription nonExistent = new Subscription("topic/abc", 1);
        routingTable.unsubscribe(clientId, Arrays.asList(nonExistent)).join();
        routingTable.unsubscribe(clientId, Arrays.asList(subscription1)).join();
        then(routingTable.topic(topicFilter2)).isNotEmpty();
    }

    @Test
    void givenMultipleClients_whenSubscribeSameTopic_thenAllSubscribersPresent() {
        String topicFilter = "topic/test";
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId1 = "clientId1";
        String clientId2 = "clientId2";
        Subscription subscription = new Subscription(topicFilter, 1);

        routingTable.subscribe(clientId1, Arrays.asList(subscription)).join();
        routingTable.subscribe(clientId2, Arrays.asList(subscription)).join();

        Topic topic = routingTable.topic(topicFilter).get();
        then(topic.subscribers()).hasSize(2);
        List<String> clientIds = topic.subscribers().stream()
            .map(Topic.Subscriber::clientId)
            .toList();
        then(clientIds).contains(clientId1, clientId2);

        // 清理
        routingTable.unsubscribe(clientId1, Arrays.asList(subscription)).join();
        then(routingTable.topic(topicFilter)).isNotEmpty(); // 应该还有clientId2的订阅

        routingTable.unsubscribe(clientId2, Arrays.asList(subscription)).join();
        then(routingTable.topic(topicFilter)).isEmpty(); // 现在应该为空
    }

    @Test
    void givenMultipleTopics_whenSubscribe_thenCorrectSubscribers() {
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        String topicFilter1 = "topic1";
        String topicFilter2 = "topic2";
        Subscription subscription1 = new Subscription(topicFilter1, 1);
        Subscription subscription2 = new Subscription(topicFilter2, 2);

        routingTable.subscribe(clientId, Arrays.asList(subscription1, subscription2)).join();

        then(routingTable.topic(topicFilter1)).isNotEmpty();
        then(routingTable.topic(topicFilter2)).isNotEmpty();

        Topic topic1 = routingTable.topic(topicFilter1).get();
        then(topic1.subscribers()).hasSize(1);
        then(topic1.subscribers().get(0).clientId()).isEqualTo(clientId);
        then(topic1.subscribers().get(0).qos()).isEqualTo(1);

        Topic topic2 = routingTable.topic(topicFilter2).get();
        then(topic2.subscribers()).hasSize(1);
        then(topic2.subscribers().get(0).clientId()).isEqualTo(clientId);
        then(topic2.subscribers().get(0).qos()).isEqualTo(2);

        // 清理
        routingTable.unsubscribe(clientId, Arrays.asList(subscription1, subscription2)).join();
        then(routingTable.topic(topicFilter1)).isEmpty();
        then(routingTable.topic(topicFilter2)).isEmpty();
    }

    @Test
    void givenNullCollections_whenSubscribeUnsubscribe_thenNoException() {
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";

        // 测试null订阅列表
        then(routingTable.subscribe(clientId, (Collection<Subscription>) null)).isCompleted();
        
        // 测试空订阅列表
        then(routingTable.subscribe(clientId, List.of())).isCompleted();
        
        // 测试null取消订阅列表
        then(routingTable.unsubscribe(clientId, (Collection<Subscription>) null)).isCompleted();
        
        // 测试空取消订阅列表
        then(routingTable.unsubscribe(clientId, List.of())).isCompleted();
    }

    @Test
    void givenExistingSubscriber_whenSubscribeAgain_thenQosUpdated() {
        String topicFilter = "topic/test";
        RoutingTableImpl routingTable = new RoutingTableImpl();
        String clientId = "clientId";
        Subscription subscription1 = new Subscription(topicFilter, 0);
        Subscription subscription2 = new Subscription(topicFilter, 2);

        // 首次订阅，QoS 0
        routingTable.subscribe(clientId, Arrays.asList(subscription1)).join();
        Topic topic = routingTable.topic(topicFilter).get();
        then(topic.subscribers()).hasSize(1);
        then(topic.subscribers().get(0).qos()).isEqualTo(0);

        // 再次订阅，QoS 2，应该覆盖之前的QoS
        routingTable.subscribe(clientId, Arrays.asList(subscription2)).join();
        topic = routingTable.topic(topicFilter).get();
        then(topic.subscribers()).hasSize(1); // 仍然只有一个订阅者
        then(topic.subscribers().get(0).qos()).isEqualTo(2); // QoS应该被更新

        // 清理
        routingTable.unsubscribe(clientId, Arrays.asList(subscription2)).join();
        then(routingTable.topic(topicFilter)).isEmpty();
    }
}