package org.example.mqtt.broker.cluster.infra.redis;


import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfiguration {

    /**
     * redis://127.0.0.1:7181
     */
    @Bean
    public static RedissonClient newRedisson(
            @Value("${mqtt.server.cluster.db.redis.url:redis://10.255.4.15:7000}") String addresses) {
        Config config = new Config();
        config.setUseScriptCache(true);
        config.useClusterServers().addNodeAddress(addresses.split(","));
        return Redisson.create(config);
    }

}
