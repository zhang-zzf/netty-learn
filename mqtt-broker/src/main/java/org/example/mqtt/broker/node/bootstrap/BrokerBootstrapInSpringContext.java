package org.example.mqtt.broker.node.bootstrap;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.config.spring.aop.TimedAopConfiguration;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.node.DefaultBroker;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
@Slf4j
@Configuration
@ComponentScan(basePackageClasses = {
        DefaultBroker.class,
        TimedAopConfiguration.class,
})
public class BrokerBootstrapInSpringContext {

    @SneakyThrows
    public static void main(String[] args) {
        Authenticator authenticator = packet -> 0x00;
        ApplicationContext context = new AnnotationConfigApplicationContext(BrokerBootstrapInSpringContext.class);
        Broker broker = context.getBean(Broker.class);
        BrokerBootstrap.startServer(authenticator, broker);
    }

}
