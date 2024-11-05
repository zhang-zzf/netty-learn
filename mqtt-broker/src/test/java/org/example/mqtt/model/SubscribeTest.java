package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
class SubscribeTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        List<Subscribe.Subscription> subscriptionList = new ArrayList<Subscribe.Subscription>() {{
            add(new Subscribe.Subscription("tbt/shanghai", 2));
            add(new Subscribe.Subscription("mqtt/shanghai", 2));
        }};
        Subscribe out = Subscribe.from(subscriptionList);
        ByteBuf packet = out.toByteBuf();
        Subscribe in = (Subscribe) Subscribe.from(packet);
        then(in.subscriptions()).hasSize(2);
    }

    /**
     * 合法 TopicFilter
     */
    @ParameterizedTest(name = "{0} is a valid TopicFilter")
    @CsvFileSource(resources = {"/subscribe/valid_topic_filter.csv"})
    void givenFuzzyTopicFilter_when_thenMatch(String topicFilter) {
        then(Subscribe.topicFilterValidate(topicFilter)).isTrue();
    }

    /**
     * 非法 TopicFilter
     */
    @ParameterizedTest(name = "{0} is not valid")
    @CsvFileSource(resources = {"/subscribe/not_valid_topic_filter.csv"})
    void givenFuzzyTopicFilter_when_thenNotMatch(String topicFilter) {
        then(Subscribe.topicFilterValidate(topicFilter)).isFalse();
    }

}