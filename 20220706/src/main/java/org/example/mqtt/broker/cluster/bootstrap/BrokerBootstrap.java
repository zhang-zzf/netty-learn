package org.example.mqtt.broker.cluster.bootstrap;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
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
        final ClusterBroker clusterBroker = new ClusterBroker(elasticsearchDbRepoImpl());
        final Cluster cluster = new Cluster(clusterBroker);
        Supplier<DefaultServerSessionHandler> handlerSupplier = () ->
                new ClusterServerSessionHandler(authenticator, 3, cluster);
        Map<String, String> protocolToUrl =
                org.example.mqtt.broker.node.bootstrap.BrokerBootstrap.startServer(handlerSupplier);
        String mqttUrl = protocolToUrl.get("mqtt");
        if (mqttUrl == null) {
            throw new UnsupportedOperationException("Cluster mode need mqtt protocol enabled");
        }
        // 开启集群节点信息同步
        cluster.localNode(mqttUrl).startSyncJob();
        String anotherNode = System.getProperty("mqtt.server.cluster.join");
        if (anotherNode != null) {
            log.info("Node({}) try to connect to anotherNode({})", clusterBroker.nodeId(), anotherNode);
            cluster.join(anotherNode);
            log.info("Node({}) connected to the anotherNode({})", clusterBroker.nodeId(), anotherNode);
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
