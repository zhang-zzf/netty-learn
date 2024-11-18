package org.example.bootstrap;

import static org.example.bootstrap.ClusterBrokerBootstrap.startBroker;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.ClusterBrokerImpl;
import org.example.mqtt.broker.cluster.ClusterBrokerState;
import org.example.mqtt.broker.cluster.infra.redis.ClusterBrokerStateImpl;
import org.example.mqtt.broker.cluster.infra.redis.RedisConfiguration;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.node.DefaultBroker;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
@Slf4j
@Configuration
public class ClusterBrokerBootstrapInSpringContext {

    @SneakyThrows
    public static void main(String[] args) {
        startSpringContext();
    }

    public static ApplicationContext startSpringContext() {
        Authenticator authenticator = packet -> 0x00;
        ApplicationContext context = new AnnotationConfigApplicationContext(ClusterBrokerBootstrapInSpringContext.class);
        Cluster cluster = context.getBean(Cluster.class);
        ClusterBroker clusterBroker = context.getBean(ClusterBroker.class);
        startBroker(authenticator, cluster, clusterBroker);
        return context;
    }

    @Bean
    public ClusterBroker clusterBroker(
        ClusterBrokerState clusterBrokerState,
        @Qualifier("nodeBroker") Broker nodeBroker,
        Cluster cluster) {
        return new ClusterBrokerImpl(clusterBrokerState, nodeBroker, cluster);
    }

    @Bean
    public Broker nodeBroker() {
        return new DefaultBroker();
    }

    @Bean
    public Cluster cluster() {
        return new Cluster();
    }

    /**
     * redis://127.0.0.1:7181
     */
    @Bean
    public RedissonClient newRedisson(
        @Value("${mqtt.server.cluster.db.redis.url:redis://10.255.4.15:7000}") String addresses) {
        return RedisConfiguration.newRedisson(addresses);
    }

    @Bean
    public ClusterBrokerState clusterBrokerState(RedissonClient redissonClient) {
        return new ClusterBrokerStateImpl(redissonClient);
    }

}
