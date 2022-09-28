package org.example.mqtt.broker.cluster.infra.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.cluster.ClusterTopic;
import org.example.mqtt.broker.cluster.infra.es.config.ElasticsearchClientConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.example.mqtt.broker.cluster.infra.es.ClusterDbRepoImpl.TOPIC_FILTER;

@Configuration
@ComponentScan(basePackageClasses = {
        ElasticsearchClientConfig.class,
        ClusterDbRepoImpl.class,
})
@Slf4j
public class ClusterDbRepoImplPressure {

    @SneakyThrows
    public static void main(String[] args) {
        // 启动打点
        ApplicationContext context = new AnnotationConfigApplicationContext(ClusterDbRepoImplPressure.class);
        ElasticsearchClient client = context.getBean(ElasticsearchClient.class);
        ClusterDbRepo dbRepo = context.getBean(ClusterDbRepo.class);
        // nodes
        List<String> nodes = new ArrayList<>(64);
        for (int i = 0; i < 32; i++) {
            nodes.add("Node" + i);
        }
        // 100W Client
        Set<String> list = new HashSet<>(2024000);
        // example: 021/
        String cIdPrefix = System.getProperty("cId.prefix");
        // 生成 100W id
        backTrace(new StringBuilder(cIdPrefix), 0, list, 6);
        // 线程池
        int maxThread = Runtime.getRuntime().availableProcessors() * 4;
        maxThread = Integer.getInteger("db.pressure.thread.num", maxThread);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, maxThread,
                60, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(maxThread),
                // 提交线程自己跑
                new ThreadPoolExecutor.CallerRunsPolicy());
        if (Boolean.getBoolean("db.pressure.search.mode")) {
            searchPressure(dbRepo, list, threadPool);
        } else {
            indexPressure(client, dbRepo, nodes, list, threadPool);
        }
    }

    private static void searchPressure(ClusterDbRepo dbRepo,
                                       Set<String> list,
                                       ThreadPoolExecutor threadPool) {
        final AtomicLong start = new AtomicLong(System.currentTimeMillis());
        AtomicLong count = new AtomicLong(0);
        AtomicLong respSize = new AtomicLong(0);
        for (String id : list) {
            Runnable task = () -> {
                // String id2 = id.substring(0, id.lastIndexOf("/") + 1) + "+/+";
                List<ClusterTopic> clusterTopics = dbRepo.matchTopic(id);
                respSize.getAndAdd(clusterTopics.size());
                count.getAndIncrement();
                if ((count.get() % 10000) == 0) {
                    long timeUsed = System.currentTimeMillis() - start.get();
                    long qps = 10000 / (timeUsed / 1000);
                    long timePerReq = timeUsed / 10000 * threadPool.getPoolSize();
                    log.info("search 1W request->{}ms/doc, qps:{}, matched: {}", timePerReq, qps, respSize.get());
                    start.set(System.currentTimeMillis());
                    respSize.set(0);
                }
            };
            threadPool.submit(task);
        }
    }

    private static void indexPressure(ElasticsearchClient client,
                                      ClusterDbRepo dbRepo,
                                      List<String> nodes,
                                      Set<String> list,
                                      ThreadPoolExecutor threadPool) throws IOException {
        if (Boolean.getBoolean("db.pressure.batch.mode")) {
            // create index if not exist
            if (!client.indices().exists(r -> r.index(TOPIC_FILTER)).value()) {
                InputStream indexCreateJSON = ClusterDbRepoImplPressure.class
                        .getResourceAsStream("/infra/es/topic_filter_create.json");
                client.indices().create(r -> r.index(TOPIC_FILTER).withJson(indexCreateJSON)).acknowledged();
            }
        } else {
            // create index every time
            if (client.indices().exists(r -> r.index(TOPIC_FILTER)).value()) {
                client.indices().delete(r -> r.index(TOPIC_FILTER));
            }
            InputStream indexCreateJSON = ClusterDbRepoImplPressure.class
                    .getResourceAsStream("/infra/es/topic_filter_create.json");
            client.indices().create(r -> r.index(TOPIC_FILTER).withJson(indexCreateJSON)).acknowledged();
        }
        final AtomicLong start = new AtomicLong(System.currentTimeMillis());
        AtomicLong count = new AtomicLong(0);
        for (String id : list) {
            Runnable task = () -> {
                // String id2 = id.substring(0, id.lastIndexOf("/") + 1) + "+/+";
                dbRepo.addNodeToTopic(randomNode(nodes), Arrays.asList(id));
                count.getAndIncrement();
                if ((count.get() % 10000) == 0) {
                    log.info("index 1W document, {}ms/doc", (System.currentTimeMillis() - start.get()) / 10000);
                    start.set(System.currentTimeMillis());
                }
            };
            threadPool.submit(task);
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
        return nodes.get(new Random().nextInt(32));
    }

}