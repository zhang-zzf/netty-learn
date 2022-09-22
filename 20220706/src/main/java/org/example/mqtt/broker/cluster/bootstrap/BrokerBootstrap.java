package org.example.mqtt.broker.cluster.bootstrap;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.cluster.ClusterServerSessionHandler;
import org.example.mqtt.broker.cluster.infra.es.ClusterDbRepoImpl;
import org.example.mqtt.broker.cluster.infra.es.config.ElasticsearchClientConfig;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;

import java.util.Map;
import java.util.function.Supplier;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
public class BrokerBootstrap {

    @SneakyThrows
    public static void main(String[] args) {
        Authenticator authenticator = packet -> 0x00;
        final Cluster cluster = new Cluster();
        final ClusterBroker clusterBroker = new ClusterBroker(elasticsearchDbRepoImpl());
        Supplier<DefaultServerSessionHandler> handlerSupplier = () ->
                new ClusterServerSessionHandler(authenticator, 3, cluster);
        Map<String, String> protocolToUrl =
                org.example.mqtt.broker.node.bootstrap.BrokerBootstrap.startServer(handlerSupplier);
        clusterBroker.listenedServer(protocolToUrl);
        // 开启集群节点信息同步
        // broker join the Cluster
        cluster.join(clusterBroker).start();
        String anotherNode = System.getProperty("mqtt.server.cluster.join");
        if (anotherNode != null) {
            log.info("Node({}) try to connect to another Node({})", clusterBroker.nodeId(), anotherNode);
            cluster.join(anotherNode);
            log.info("Node({}) connected to the another Node({})", clusterBroker.nodeId(), anotherNode);
            log.info("Cluster.Nodes->{}", JSON.toJSONString(cluster.nodes()));
        }
        log.info("Node({}) start success", clusterBroker.nodeId());
    }

    private static ClusterDbRepo elasticsearchDbRepoImpl() {
        String esUrl = System.getProperty("mqtt.server.cluster.db.impl.es.url", "http://nudocker01:9120");
        String username = System.getProperty("mqtt.server.cluster.db.impl.es.username", "elastic");
        String password = System.getProperty("mqtt.server.cluster.db.impl.es.password", "8E78NY1mnfGvQJ6e7aHy");
        ElasticsearchClientConfig config = new ElasticsearchClientConfig();
        ElasticsearchClient client = config.elasticsearchClient(esUrl, username, password);
        return new ClusterDbRepoImpl(client);
    }

}
