package org.example.mqtt.broker.cluster.bootstrap;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.cluster.ClusterServerSessionHandler;
import org.example.mqtt.broker.cluster.infra.es.ClusterDbRepoImpl;
import org.example.mqtt.broker.cluster.infra.es.config.ElasticsearchClientConfig;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.function.Supplier;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
@Configuration
@ComponentScan({"org.example.mqtt.broker.cluster"})
public class BrokerBootstrapInSpringContext {

    @SneakyThrows
    public static void main(String[] args) {
        Authenticator authenticator = packet -> 0x00;
        ApplicationContext context = new AnnotationConfigApplicationContext(BrokerBootstrapInSpringContext.class);
        Cluster cluster = context.getBean(Cluster.class);
        ClusterBroker clusterBroker = context.getBean(ClusterBroker.class);
        startBroker(authenticator, cluster, clusterBroker);
    }

    @SneakyThrows
    public static void startBroker(Authenticator authenticator,
                                   Cluster cluster,
                                   ClusterBroker clusterBroker) {
        Supplier<DefaultServerSessionHandler> handlerSupplier = () ->
                new ClusterServerSessionHandler(authenticator, 3, cluster);
        Map<String, Broker.ListenPort> protocolToUrl =
                org.example.mqtt.broker.node.bootstrap.BrokerBootstrap.startServer(handlerSupplier);
        clusterBroker.listenedServer(protocolToUrl);
        // 开启集群节点信息同步
        // broker join the Cluster
        cluster.join(clusterBroker).start();
        String anotherNode = System.getProperty("mqtt.server.cluster.join");
        if (anotherNode != null) {
            log.info("Cluster try to connect to another Node->{}", anotherNode);
            cluster.join(anotherNode);
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
