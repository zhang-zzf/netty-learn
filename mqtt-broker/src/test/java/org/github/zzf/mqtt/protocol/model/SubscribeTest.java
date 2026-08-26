package org.github.zzf.mqtt.protocol.model;

import static org.assertj.core.api.BDDAssertions.then;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.splitSlashSeparateStr;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription.V50;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

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
            add(new Subscribe.Subscription("tbt/shanghai", (byte) 2));
            add(new Subscribe.Subscription("mqtt/shanghai", (byte) 2));
        }};
        Subscribe out = Subscribe.from(subscriptionList);
        ByteBuf packet = out.toByteBuf();
        Subscribe in = (Subscribe) Subscribe.from(packet);
        then(in.subscriptions()).hasSize(2);
    }

    /**
     * TopicFilter / TopicName 分隔
     */
    @Test
    void givenSlashSeparateString_when_then() {
        // mqtt 要求 "/" 是 "" "" 2级结构
        then(splitSlashSeparateStr("/")).containsExactly("", "");
        then(splitSlashSeparateStr("/a")).containsExactly("", "a");
        then(splitSlashSeparateStr("a/")).containsExactly("a", "");
        // watch out:
        then("/".split("/")).containsExactly();
        // watch out:
        then("/a".split("/")).containsExactly("", "a");
        // watch out:
        then("a/".split("/")).containsExactly("a");
        then("a/ ".split("/")).containsExactly("a", " ");
    }

    /**
     * 合法 TopicFilter
     */
    @ParameterizedTest(name = "{0} is a valid TopicFilter")
    @CsvFileSource(resources = {"/subscribe/valid_topic_filter.csv"})
    void givenFuzzyTopicFilter_when_thenMatch(String topicFilter) {
        then(new Subscription(topicFilter, (byte) 0x00).validate()).isTrue();
    }

    /**
     * 非法 TopicFilter
     */
    @ParameterizedTest(name = "{0} is not valid")
    @CsvFileSource(resources = {"/subscribe/not_valid_topic_filter.csv"})
    void givenFuzzyTopicFilter_when_thenNotMatch(String topicFilter) {
        then(new Subscription(topicFilter, (byte)0x00).validate()).isFalse();
    }


    @Test void givenSharedSubscriptionTopicFilter_when_then() {
        V50 v50 = new V50("$share/group/a/bc", (byte) 0x00, null);
        then(v50.sharedFilter()).isEqualTo("a/bc");
    }

}