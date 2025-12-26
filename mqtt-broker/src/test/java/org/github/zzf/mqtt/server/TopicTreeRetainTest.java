package org.github.zzf.mqtt.server;

import static org.assertj.core.api.BDDAssertions.then;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

class TopicTreeRetainTest {

    /**
     * Topic 匹配测试
     */
    @ParameterizedTest(name = "{0} match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenMatch(String topicName, String topicFilter) {
        try (TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest")) {
            Publish publish = Publish.outgoing(1, topicName, Unpooled.copiedBuffer("test payload", java.nio.charset.StandardCharsets.UTF_8));
            retainManager.add(publish).join();
            List<Publish> result = retainManager.match(topicFilter);
            then(result).isNotEmpty();
            then(result.get(0).topicName()).isEqualTo(topicName);
            // 清理
            retainManager.del(publish).join();
        }
    }

    /**
     * Topic 不匹配测试
     */
    @ParameterizedTest(name = "{0} will not match {1}")
    @CsvFileSource(resources = {"/broker/topic_name_topic_filter_not_match.csv"})
    void given_whenTopicNameMatchTopicFilter_thenNotMatch(String topicName, String topicFilter) {
        try (TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest")) {
            Publish publish = Publish.outgoing(1, topicName, Unpooled.copiedBuffer("test payload", java.nio.charset.StandardCharsets.UTF_8));
            retainManager.add(publish).join();
            List<Publish> result = retainManager.match(topicFilter);
            then(result).isEmpty();
            // 清理
            retainManager.del(publish).join();
        }
    }

    @Test
    void givenEmpty_whenTopic_then() {
        try (TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest")) {
            then(retainManager.match("topic/abc")).isEmpty();
        }
    }

    @Test
    void givenMultipleRetainedMessages_whenMatch_thenAllMatched() {
        TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest");
        String tn1 = "sport/tennis/player1";
        String tn2 = "sport/tennis/player1/ranking";
        String tn3 = "sport/tennis/player2";

        Publish publish1 = Publish.outgoing(1, tn1, Unpooled.copiedBuffer("payload1", java.nio.charset.StandardCharsets.UTF_8));
        Publish publish2 = Publish.outgoing(1, tn2, Unpooled.copiedBuffer("payload2", java.nio.charset.StandardCharsets.UTF_8));
        Publish publish3 = Publish.outgoing(1, tn3, Unpooled.copiedBuffer("payload3", java.nio.charset.StandardCharsets.UTF_8));

        // 添加保留消息
        retainManager.add(publish1, publish2, publish3).join();

        // 测试通配符匹配
        List<Publish> result = retainManager.match("sport/tennis/#");
        then(result).hasSize(3); // 应该匹配到所有三个消息
        List<String> topicNames = result.stream().map(Publish::topicName).toList();
        then(topicNames).contains(tn1, tn2, tn3);

        // 清理
        retainManager.del(publish1, publish2, publish3).join();
    }


    @Test
    void givenEmptyVarargs_whenAddMatchDel_thenNoException() {
        TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest");
        // 测试空参数
        then(retainManager.add()).isCompleted();
        then(retainManager.match()).isCompleted();
        then(retainManager.del()).isCompleted();
    }

    @Test
    void givenSameTopic_whenAddMultipleTimes_thenLatestRetained() {
        TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest");
        String topicName = "test/topic";

        Publish publish1 = Publish.outgoing(1, topicName, Unpooled.copiedBuffer("first payload", java.nio.charset.StandardCharsets.UTF_8));
        Publish publish2 = Publish.outgoing(2, topicName, Unpooled.copiedBuffer("second payload", java.nio.charset.StandardCharsets.UTF_8));

        // 添加第一个消息
        retainManager.add(publish1).join();
        List<Publish> result = retainManager.match(topicName);
        then(result).hasSize(1);
        then(result.get(0).payload().toString(java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("first payload");

        // 添加第二个消息（相同主题），会覆盖第一个
        retainManager.add(publish2).join();
        result = retainManager.match(topicName);
        then(result).hasSize(1);
        then(result.get(0).payload().toString(java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("second payload");
        then(result.get(0).qos()).isEqualTo(2);

        // 清理
        retainManager.del(publish2).join();
        result = retainManager.match(topicName);
        then(result).isEmpty();
    }

    @Test
    void givenMultipleTopic_whenMatchMultiWild_thenSuccess() {
        try (TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest")) {
            String tn0 = "sport";
            String tn10 = "sport/player1";
            String tn1 = "sport/player1/rank";
            String tn11 = "sport/player1/rank2";
            String tn2 = "sport/player3/rank";
            String tn3 = "sport/player4/rank";
            ByteBuf payload = Unpooled.copiedBuffer("first payload", StandardCharsets.UTF_8);
            retainManager.add(Publish.outgoing(1, tn0, payload)).join();
            retainManager.add(Publish.outgoing(1, tn10, payload)).join();
            retainManager.add(Publish.outgoing(1, tn1, payload)).join();
            retainManager.add(Publish.outgoing(1, tn11, payload)).join();
            retainManager.add(Publish.outgoing(1, tn2, payload)).join();
            retainManager.add(Publish.outgoing(1, tn3, payload)).join();
            // when
            List<Publish> result = retainManager.match("sport/#");
            // then
            then(result).hasSize(6);
            then(result.stream().map(Publish::topicName)).contains(tn0, tn10, tn1, tn11, tn2, tn3);
        }
    }

    @Test
    void givenMultipleTopic_whenMatchSingleWild_thenSuccess() {
        try (TopicTreeRetain retainManager = new TopicTreeRetain("TopicTreeRetainTest")) {
            String tn0 = "sport";
            String tn10 = "sport/player1";
            String tn1 = "sport/player1/rank";
            String tn11 = "sport/player1/rank2";
            String tn2 = "sport/player3/rank";
            String tn3 = "sport/player4/rank";
            ByteBuf payload = Unpooled.copiedBuffer("first payload", StandardCharsets.UTF_8);
            retainManager.add(Publish.outgoing(1, tn0, payload)).join();
            retainManager.add(Publish.outgoing(1, tn10, payload)).join();
            retainManager.add(Publish.outgoing(1, tn1, payload)).join();
            retainManager.add(Publish.outgoing(1, tn11, payload)).join();
            retainManager.add(Publish.outgoing(1, tn2, payload)).join();
            retainManager.add(Publish.outgoing(1, tn3, payload)).join();
            // when
            // then
            then(retainManager.match("sport/+").stream().map(Publish::topicName))
                .containsOnly(tn10);
            //
            // when
            // then
            then(retainManager.match("sport/+/+").stream().map(Publish::topicName))
                .containsOnly(tn1, tn11, tn2, tn3);
            //
            // when
            // then
            then(retainManager.match("sport/+/#").stream().map(Publish::topicName))
                .containsOnly(tn10, tn1, tn11, tn2, tn3);
        }
    }

}