package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/23
 */
@Data
@Accessors(chain = true)
public class Message {

    private String topic;
    private ByteBuf payload;
    private int qos;

    private static final int STATUS_INIT = 1 << 0;
    private static final int STATUS_SENT = 1 << 1;
    private static final int STATUS_CLIENT_RECEIVED = 1 << 2;
    private static final int STATUS_COMPLETE = 3 << 3;

    private int messageStatus = STATUS_INIT;


}
