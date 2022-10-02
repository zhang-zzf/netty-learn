package org.example.mqtt.broker.cluster.bootstrap;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.config.micrometer.spring.aop.TimedAopConfiguration;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.cluster.ClusterBroker;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import static org.example.mqtt.broker.cluster.bootstrap.BrokerBootstrap.startBroker;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
@Configuration
@ComponentScan(basePackageClasses = {
        ClusterBroker.class,
        TimedAopConfiguration.class,
})
public class BrokerBootstrapInSpringContext {

    @SneakyThrows
    public static void main(String[] args) {
        startSpringContext();
    }

    public static ApplicationContext startSpringContext() {
        Authenticator authenticator = packet -> 0x00;
        ApplicationContext context = new AnnotationConfigApplicationContext(BrokerBootstrapInSpringContext.class);
        Cluster cluster = context.getBean(Cluster.class);
        ClusterBroker clusterBroker = context.getBean(ClusterBroker.class);
        startBroker(authenticator, cluster, clusterBroker);
        return context;
    }

}
