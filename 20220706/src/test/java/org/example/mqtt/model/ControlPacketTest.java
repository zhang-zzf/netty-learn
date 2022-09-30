package org.example.mqtt.model;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.model.ControlPacket.hexPId;
import static org.example.mqtt.model.ControlPacket.hexPIdToShort;

@Slf4j
class ControlPacketTest {

    @Test
    void given_whenHexAndDeHex_then() {
        AtomicLong cnt = new AtomicLong(0);
        for (short i = Short.MIN_VALUE;; i++) {
            then(hexPIdToShort(hexPId(i))).isEqualTo(i);
            cnt.getAndIncrement();
            if (i == Short.MAX_VALUE) {
                break;
            }
        }
        then(cnt.get()).isEqualTo(65536L);
    }

}