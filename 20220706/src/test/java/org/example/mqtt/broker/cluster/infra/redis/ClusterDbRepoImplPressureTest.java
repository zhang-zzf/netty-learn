package org.example.mqtt.broker.cluster.infra.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@Slf4j
class ClusterDbRepoImplPressureTest {

    @Test
    void given_when_then() {
        Map<Integer, Long> keys = new HashMap<>(16384);
        for (int i = 0; i < 50; i++) {
            for (int j = 0; j < 100; j++) {
                String key = String.valueOf(i) + j;
                int slot = ClusterDbRepoImplPressure.calcSlot(key);
                keys.put(slot, keys.getOrDefault(slot, 0L) + 1);
            }
        }
        List<Map.Entry<Integer, Long>> sorted = keys.entrySet().stream()
                .sorted(Comparator.<Map.Entry<Integer, Long>, Long>comparing(Map.Entry::getValue).reversed())
                .collect(toList());
        int cnt = 0;
        for (Map.Entry<Integer, Long> e : sorted) {
            if (e.getKey() < 683) {
                cnt += e.getValue();
            }
        }
        log.info("{} -> {}", cnt, cnt * 100d / 5000);
    }


    @Test
    void gvt() {
        String key = "{2714}/271/8/8/0";
        int slot = ClusterDbRepoImplPressure.calcSlot(key);
        log.info("slot: {} -> {}", key, slot);
    }

}