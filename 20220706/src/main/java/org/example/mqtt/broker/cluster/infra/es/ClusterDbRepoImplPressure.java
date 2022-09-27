package org.example.mqtt.broker.cluster.infra.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.cluster.infra.es.config.ElasticsearchClientConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

import static org.example.mqtt.broker.cluster.infra.es.ClusterDbRepoImpl.TOPIC_FILTER;

@Slf4j
public class ClusterDbRepoImplPressure {

    static ClusterDbRepoImpl dbRepo;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        ElasticsearchClientConfig config = new ElasticsearchClientConfig();
        ElasticsearchClient client = config.elasticsearchClient("http://nudocker01:9120", "elastic", "8E78NY1mnfGvQJ6e7aHy");
        dbRepo = new ClusterDbRepoImpl(client);
        // delete topic_filter
        if (client.indices().exists(r -> r.index(TOPIC_FILTER)).value()) {
            client.indices().delete(r -> r.index(TOPIC_FILTER));
        }
        InputStream indexCreateJSON = ClusterDbRepoImplPressure.class
                .getResourceAsStream("/infra/es/topic_filter_create.json");
        client.indices().create(r -> r.index(TOPIC_FILTER).withJson(indexCreateJSON)).acknowledged();
        List<String> nodes = new ArrayList<>(64);
        for (int i = 0; i < 32; i++) {
            nodes.add("Node" + i);
        }
        int maxThread = Runtime.getRuntime().availableProcessors() * 4;
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, maxThread,
                60, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(maxThread),
                // 提交线程自己跑
                new ThreadPoolExecutor.CallerRunsPolicy());
        // 1000W Client
        Set<String> list = new HashSet<>(10240000);
        // 生成 id
        backTrace(new StringBuilder(), 0, list, 6);
        long start = System.currentTimeMillis();
        Future<?> last = null;
        for (String id : list) {
            Runnable task = () -> {
                // String id2 = id.substring(0, id.lastIndexOf("/") + 1) + "+/+";
                dbRepo.addNodeToTopic(randomNode(nodes), Arrays.asList(id));
            };
            last = threadPool.submit(task);
        }
        // 最后一个提交的任务执行完成
        last.get();
        log.info("useTime: {}ms", System.currentTimeMillis() - start);
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