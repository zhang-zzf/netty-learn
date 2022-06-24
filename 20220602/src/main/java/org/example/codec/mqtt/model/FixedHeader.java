package org.example.codec.mqtt.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/22
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class FixedHeader {

    public static final byte TYPE_CONNECT = 0x00;
    public static final byte TYPE_PUBLISH = 0x03;

    private byte byte_1;
    private long remainingLength;

}
