package org.example.micrometer.utils;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MetricUtil {

    public static volatile boolean metricOn = true;

    private MetricUtil() {

    }

    private static final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    /**
     * Timer 监控
     *
     * @param metricName "指标名"
     * @param value 要记录的 count 值，默认单位 TimeUnit.MILLISECONDS
     * @param tags 对应业务监控中的 "字段"。可以为空。
     */
    public static final void time(String metricName, long value, String... tags) {
        if (!metricOn) {
            return;
        }
        String id = id(metricName, tags);
        Timer timer = timers.get(id);
        if (timer == null) {
            timers.putIfAbsent(id, Timer.builder(metricName)
                    .tags(tags)
                    .publishPercentileHistogram()
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofSeconds(10))
                    .register(Metrics.globalRegistry));
            timer = timers.get(id);
        }
        timer.record(value, TimeUnit.MILLISECONDS);
    }

    private static String id(String metricName, String[] tags) {
        StringBuilder buf = new StringBuilder(metricName);
        for (String tag : tags) {
            buf.append(tag);
        }
        return buf.toString();
    }

    /**
     * Counter 监控
     * <p>value == 1 即为记录的QPS</p>
     *
     * @param metricName "指标名"
     * @param tags 对应业务监控中的 "字段"。可以为空。
     */
    public static final void count(String metricName, String... tags) {
        if (!metricOn) {
            return;
        }
        Metrics.counter(metricName, tags).increment(1);
    }

    /**
     * Counter 监控
     * <p>value == 1 即为记录的QPS</p>
     *
     * @param metricName "指标名"
     * @param value 要记录的 count 值
     * @param tags 对应业务监控中的 "字段"。可以为空。
     */
    public static final void count(String metricName, int value, String... tags) {
        if (!metricOn) {
            return;
        }
        Metrics.counter(metricName, tags).increment(value);
    }

    public static final void gauge(String metricName, Map map) {
        if (!metricOn) {
            return;
        }
        Metrics.gauge(metricName, map, Map::size);
    }

    public static final void gauge(String metricName, long val, String... tags) {
        if (!metricOn) {
            return;
        }
        String id = id(metricName, tags);
        AtomicLong gauge = gauges.get(id);
        if (gauge == null) {
            AtomicLong atomicLong = Metrics.gauge(metricName, new AtomicLong(0));
            gauges.putIfAbsent(id, atomicLong);
            gauge = gauges.get(id);
        }
        gauge.set(val);
    }

}
