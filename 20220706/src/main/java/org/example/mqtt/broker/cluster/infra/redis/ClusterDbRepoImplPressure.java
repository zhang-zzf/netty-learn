package org.example.mqtt.broker.cluster.infra.redis;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.config.micrometer.spring.aop.TimedAopConfiguration;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.cluster.ClusterTopic;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

@Configuration
@ComponentScan(basePackageClasses = {
        ClusterBroker.class,
        TimedAopConfiguration.class,
})
@Slf4j
public class ClusterDbRepoImplPressure {

    @SneakyThrows
    public static void main(String[] args) {
        // 打点
        ApplicationContext context = new AnnotationConfigApplicationContext(ClusterDbRepoImplPressure.class);
        ClusterDbRepo dbRepo = context.getBean(ClusterDbRepo.class);
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
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, maxThread,
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
                threadPool.submit(task);
            }
        } else if (Boolean.getBoolean("db.pressure.mode.search")) {
            for (String id : list) {
                Runnable task = () -> {
                    List<ClusterTopic> clusterTopics = dbRepo.matchTopic(id);
                    if (clusterTopics.size() != 1) {
                        log.info("matchTopic failed-> req:{}, resp: {}", id, clusterTopics);
                    }
                    String id2 = id + "/publish";
                    List<ClusterTopic> topics = dbRepo.matchTopic(id2);
                    if (topics.size() != 1) {
                        log.info("matchTopic failed-> req:{}, resp: {}", id, clusterTopics);
                    }
                };
                threadPool.submit(task);
            }
        } else {
            for (String id : list) {
                Runnable task = () -> {
                    String id2 = id.substring(0, id.lastIndexOf("/") + 1) + "+/+";
                    dbRepo.addNodeToTopic(randomNode(nodes), asList(id, id2));
                };
                threadPool.submit(task);
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

}