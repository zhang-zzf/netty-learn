package org.example.codec.mqtt.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/23
 */
@Data
@Accessors(chain = true)
public class Subscription {

    private String topic;
    private int qos;

}
