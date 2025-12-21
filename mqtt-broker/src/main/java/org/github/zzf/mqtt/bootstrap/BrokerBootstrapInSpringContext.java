package org.github.zzf.mqtt.bootstrap;

import io.micrometer.core.aop.TimedAspect;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.session.server.Authenticator;
import org.github.zzf.mqtt.protocol.session.server.Broker;
import org.github.zzf.mqtt.mqtt.broker.node.DefaultBroker;
import org.github.zzf.mqtt.mqtt.broker.node.DefaultServerSessionHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
@Slf4j
@Configuration
public class BrokerBootstrapInSpringContext {

    @SneakyThrows
    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(BrokerBootstrapInSpringContext.class);
        Broker broker = context.getBean(Broker.class);
        BrokerBootstrap.startServer(() -> new DefaultServerSessionHandler(broker, 3));
    }

    @Bean
    public Broker defaultBroker() {
        return new DefaultBroker(packet -> 0x00);
    }

    @Configuration
    @EnableAspectJAutoProxy
    public static class TimedAopConfiguration {

        @Bean
        public TimedAspect timedAspect() {
            return new TimedAspect();
        }

    }

}
