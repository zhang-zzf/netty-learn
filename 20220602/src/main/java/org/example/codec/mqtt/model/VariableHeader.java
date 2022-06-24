package org.example.codec.mqtt.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/22
 */
@Data
@Accessors(chain = true)
public class VariableHeader {

    public static VariableHeader newPacketIdentifier(short packetId) {
        return null;
    }
}
