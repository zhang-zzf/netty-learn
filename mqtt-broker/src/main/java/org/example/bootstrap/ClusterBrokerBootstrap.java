package org.example.bootstrap;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterBrokerImpl;
import org.example.mqtt.broker.cluster.ClusterBrokerState;
import org.example.mqtt.broker.cluster.ClusterServerSessionHandler;
import org.example.mqtt.broker.cluster.infra.redis.ClusterBrokerStateImpl;
import org.example.mqtt.broker.cluster.infra.redis.RedisConfiguration;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.node.DefaultBroker;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;

import java.util.function.Supplier;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
@Slf4j
public class ClusterBrokerBootstrap {

    @SneakyThrows
    public static void main(String[] args) {
        if (!Boolean.getBoolean("spring.enable")) {
            Authenticator authenticator = packet -> 0x00;
            final Cluster cluster = new Cluster();
            final ClusterBroker clusterBroker = new ClusterBrokerImpl(redisClusterDbRepo(), new DefaultBroker(), cluster);
            startBroker(authenticator, cluster, clusterBroker);
        } else {
            log.info("start BrokerBootstrap with Spring Context");
            ClusterBrokerBootstrapInSpringContext.main(args);
        }
    }

    @SneakyThrows
    public static void startBroker(Authenticator authenticator,
                                   Cluster cluster,
                                   ClusterBroker clusterBroker) {
        Supplier<DefaultServerSessionHandler> handlerSupplier = () ->
                new ClusterServerSessionHandler(authenticator, 3, cluster);
        org.example.bootstrap.BrokerBootstrap.startServer(handlerSupplier);
        // 开启集群节点信息同步
        // broker join the Cluster
        cluster.bind(clusterBroker).start();
        String anotherNode = System.getProperty("mqtt.server.cluster.join");
        if (anotherNode != null) {
            log.info("startBroker try to connect to another Node-> Node: {}", anotherNode);
            cluster.join(anotherNode);
            log.debug("startBroker Cluster.Nodes-> Cluster: {}", cluster.nodes());
        }
        log.info("startBroker success-> nodeId: {}", clusterBroker.nodeId());
    }

    private static ClusterBrokerState redisClusterDbRepo() {
        String redisAddresses = "redis://10.255.4.15:7000, redis://10.255.4.15:7001, redis://10.255.4.15:7002";
        redisAddresses = System.getProperty("mqtt.server.cluster.db.redis.url", redisAddresses);
        return new ClusterBrokerStateImpl(RedisConfiguration.newRedisson(redisAddresses));
    }

}
