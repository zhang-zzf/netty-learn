package org.example.mqtt.broker.cluster.infra.redis;

import org.example.mqtt.broker.cluster.ClusterTopic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.redisson.api.RedissonClient;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.BDDAssertions.then;

class ClusterDbRepoImplTest {

    final static RedissonClient client;

    static {
        String addresses = "redis://10.255.4.15:7000, redis://10.255.4.15:7001,redis://10.255.4.15:7002";
        client = RedisConfiguration.newRedisson(addresses);
    }

    final static ClusterDbRepoImpl impl = new ClusterDbRepoImpl(client);

    @Test
    void givenTopicFilter_whenConvertToRedisKey_then() {
        ClusterDbRepoImpl impl = new ClusterDbRepoImpl(null);
        then(impl.convertToRedisKey("topic")).isEqualTo("{topic}");
        then(impl.convertToRedisKey("topic/ab")).isEqualTo("{topic}/ab");
        // then(impl.convertToRedisKey("topic/ab/")).isEqualTo("{topic}/ab/");
    }

    @ParameterizedTest
    @CsvFileSource(resources = {"/topicFilter/topicFilter.csv"})
    void givenTopicFilter_whenAddNodeToTopic_then(String tf) {
        ClusterDbRepoImpl impl = new ClusterDbRepoImpl(client);
        impl.addNodeToTopic("node01", asList(tf));
    }

    @ParameterizedTest
    @CsvFileSource(resources = {"/topicFilter/topicFilter.csv"})
    void givenTopicFilter_whenRemoveNodeFromTopic_then(String tf) {
        impl.removeNodeFromTopic("node01", asList(tf));
    }

    @Test
    void given_whenAddAndRemove1_then() {
        String topic = "topic/abc/de/mn";
        impl.addNodeToTopic("nod3", asList(topic));
        impl.removeNodeFromTopic("nod3", asList(topic));
        then(impl.matchTopic(topic)).isEmpty();
    }

    @Test
    void given_whenAddAndRemove2_then() {
        String topic1 = "topic/abc/de/mn";
        impl.addNodeToTopic("nod3", asList(topic1));
        String topic2 = "topic/abc";
        impl.addNodeToTopic("nod3", asList(topic2));
        impl.removeNodeFromTopic("nod3", asList(topic1));
        then(impl.matchTopic(topic1)).isEmpty();
        then(impl.matchTopic(topic2)).isNotEmpty();
        impl.removeNodeFromTopic("nod3", asList(topic2));
    }

    @Test
    void given_whenAddAndRemove3_then() {
        String topic1 = "topic/abc/de/mn";
        impl.addNodeToTopic("nod3", asList(topic1));
        String topic2 = "topic/abc";
        impl.addNodeToTopic("nod3", asList(topic2));
        impl.removeNodeFromTopic("nod3", asList(topic2));
        then(impl.matchTopic(topic2)).isEmpty();
        then(impl.matchTopic(topic1)).isNotEmpty();
        impl.removeNodeFromTopic("nod3", asList(topic1));
    }

    @Test
    void givenTopicFilter_whenMatchTopic_then() {
        ClusterDbRepoImpl impl = new ClusterDbRepoImpl(client);
        List<ClusterTopic> resp = impl.matchTopic("topic/abc/de");
        then(resp).isNotNull();
    }

}