package org.example.mqtt.broker.codec;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.BDDAssertions.then;

class SecureMqttServerInitializerTest {

    @Test
    void givenSupplier_whenGet_then() {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        Supplier<Integer> supplier = atomicInteger::getAndIncrement;
        then(supplier.get()).isEqualTo(0);
        then(supplier.get()).isEqualTo(1);
    }

}