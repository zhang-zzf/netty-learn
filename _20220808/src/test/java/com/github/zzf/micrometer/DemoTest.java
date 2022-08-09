package com.github.zzf.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author zhanfeng.zhang
 * @date 2022/04/10
 */
@ExtendWith(MockitoExtension.class)
public class DemoTest {

    @Test
    void givenSimpleMeterRegistry_when_then() {
        MeterRegistry registry = new SimpleMeterRegistry();
        Counter c = registry.counter("com.github.zzf");
        c.increment();
        c.increment();
        then(c.count()).isCloseTo(2.0d, Offset.offset(0.1d));
    }


    @Test
    void givenGlobalRegister_whenCounter_then() {
        // 添加到 globalRegister
        Metrics.addRegistry(new SimpleMeterRegistry());
        // 使用 globalRegister
        Counter counter = Metrics.counter("com.github.zzf");
        counter.increment();
        then(counter.count()).isCloseTo(1., Offset.offset(0.1));
    }

    final MeterRegistry registry = new SimpleMeterRegistry();
    /**
     * // may be wired at first
     * Attempting to construct a gauge with a primitive number or
     * one of its java.lang object forms is always incorrect.
     * These numbers are immutable. Thus, the gauge cannot ever be changed.
     * Attempting to “re-register” the gauge with a different number does not work,
     * as the registry maintains only one meter for each unique combination of name and tags.
     */
    final AtomicLong gauge = registry.gauge("com.github.zzf.gauge", new AtomicLong(0));

    @Test
    void givenGlobalRegister_whenGauge_then() {
        gauge.set(10);
        gauge.set(15);
    }

    final Timer timer = registry.timer("com.github.zzf.timer");

    @Test
    void given_whenTimer_then() {
        timer.record(1, TimeUnit.MILLISECONDS);
        timer.record(5, TimeUnit.MILLISECONDS);
        then(timer.count()).isEqualTo(2);
    }

}
