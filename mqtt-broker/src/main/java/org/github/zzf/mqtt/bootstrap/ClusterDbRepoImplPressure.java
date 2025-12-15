package org.github.zzf.mqtt.bootstrap;

import static java.util.Arrays.asList;
import static org.redisson.connection.MasterSlaveConnectionManager.MAX_SLOT;

import io.micrometer.core.aop.TimedAspect;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.mqtt.broker.cluster.ClusterBrokerState;
import org.github.zzf.mqtt.mqtt.broker.cluster.ClusterTopic;
import org.redisson.connection.CRC16;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@Slf4j
public class ClusterDbRepoImplPressure {

    @SneakyThrows
    public static void main(String[] args) {
        // 打点
        ApplicationContext context = new AnnotationConfigApplicationContext(ClusterDbRepoImplPressure.class);
        ClusterBrokerState dbRepo = context.getBean(ClusterBrokerState.class);
        // nodes
        List<String> nodes = new ArrayList<>(64);
        for (int i = 0; i < 10; i++) {
            nodes.add("Node" + i);
        }
        // 100W Client
        Set<String> list = new HashSet<>(2024000);
        // example: 021/
        String cIdPrefix = System.getProperty("cId.prefix", "021/");
        // 生成 100W id
        Integer levels = Integer.getInteger("topic.level", 2);
        backTrace(new StringBuilder(cIdPrefix), 0, list, levels);
        log.info("tf size: {}", list.size());
        // 线程池
        int maxThread = Runtime.getRuntime().availableProcessors() * 4;
        maxThread = Integer.getInteger("db.pressure.thread.num", maxThread);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(maxThread, maxThread,
                60, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(maxThread),
                // 提交线程自己跑
                new ThreadPoolExecutor.CallerRunsPolicy());
        if (Boolean.getBoolean("db.pressure.mode.clean")) {
            for (String id : list) {
                Runnable task = () -> {
                    String id2 = id.substring(0, id.lastIndexOf("/") + 1) + "+/+";
                    dbRepo.removeTopic(asList(id, id2));
                };
                if (maxThread == 1) {
                    // 单线程压测，自己跑呗
                    task.run();
                } else {
                    threadPool.submit(task);
                }
            }
        } else if (Boolean.getBoolean("db.pressure.mode.search")) {
            for (String id : list) {
                // id = 1/2/3/4/5
                for (int i = 0; i < 100; i++) {
                    // prefixId = 00/1/2/3/4/5
                    // prefixId = 01/1/2/3/4/5
                    // prefixId = 011/1/2/3/4/5
                    // prefixId = 099/1/2/3/4/5
                    String prefixId = new StringBuilder(cIdPrefix).append(i).append("/").append(id).toString();
                    Runnable task = () -> {
                        List<ClusterTopic> clusterTopics = dbRepo.matchTopic(prefixId);
                        if (clusterTopics.size() != 1) {
                            log.info("matchTopic failed-> req:{}, resp: {}", prefixId, clusterTopics);
                        }
                        String id2 = prefixId + "/publish";
                        List<ClusterTopic> topics = dbRepo.matchTopic(id2);
                        if (topics.size() != 1) {
                            log.info("matchTopic failed-> req:{}, resp: {}", id2, clusterTopics);
                        }
                    };
                    if (maxThread == 1) {
                        // 单线程压测，自己跑呗
                        task.run();
                    } else {
                        threadPool.submit(task);
                    }
                }
            }
        } else {
            for (String id : list) {
                // id = 1/2/3/4/5
                for (int i = 0; i < 100; i++) {
                    // prefixId = 00/1/2/3/4/5
                    // prefixId = 01/1/2/3/4/5
                    // prefixId = 011/1/2/3/4/5
                    // prefixId = 099/1/2/3/4/5
                    String prefixId = new StringBuilder(cIdPrefix).append(i).append("/").append(id).toString();
                    Runnable task = () -> {
                        // id2 = 00/1/2/3/4/5/+/+
                        String id2 = prefixId.substring(0, prefixId.lastIndexOf("/") + 1) + "+/+";
                        dbRepo.addNodeToTopic(randomNode(nodes), asList(prefixId, id2));
                    };
                    if (maxThread == 1) {
                        // 单线程压测，自己跑呗
                        task.run();
                    } else {
                        threadPool.submit(task);
                    }
                }
            }
        }
    }

    private static void backTrace(StringBuilder buf, int level, Set<String> list, int totalLevel) {
        if (level == totalLevel) {
            list.add(buf.substring(0, buf.length() - 1));
            return;
        }
        for (int i = 0; i < 10; i++) {
            int tLevel = i;
            int length = buf.length();
            buf.append(tLevel).append('/');
            backTrace(buf, level + 1, list, totalLevel);
            buf.delete(length, buf.length());
        }
    }

    private static String randomNode(List<String> nodes) {
        return nodes.get(new Random().nextInt(nodes.size()));
    }

    public static int calcSlot(String key) {
        if (key == null) {
            return 0;
        }
        int start = key.indexOf('{');
        if (start != -1) {
            int end = key.indexOf('}');
            if (end != -1 && start + 1 < end) {
                key = key.substring(start + 1, end);
            }
        }
        int result = CRC16.crc16(key.getBytes()) % MAX_SLOT;
        log.debug("slot {} for {}", result, key);
        return result;
    }

    @Configuration
    @EnableAspectJAutoProxy
    public static class TimedAopConfiguration {

        @Bean
        public TimedAspect timedAspect() {
            return new TimedAspect();
        }

    }

}