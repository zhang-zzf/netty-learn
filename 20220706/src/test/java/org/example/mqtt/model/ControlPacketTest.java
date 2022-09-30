package org.example.mqtt.model;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.model.ControlPacket.hexPId;
import static org.example.mqtt.model.ControlPacket.hexPIdToShort;

class ControlPacketTest {

    @Test
    @Disabled
    void given_whenHexAndDeHex_then() {
        for (short i = Short.MIN_VALUE; i <= Short.MAX_VALUE ; i++) {
            then(hexPIdToShort(hexPId(i))).isEqualTo(i);
        }
    }

}