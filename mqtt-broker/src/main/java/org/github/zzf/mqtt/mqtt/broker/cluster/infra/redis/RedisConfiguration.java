package org.github.zzf.mqtt.mqtt.broker.cluster.infra.redis;


import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
public class RedisConfiguration {

    /**
     * redis://127.0.0.1:7181
     */
    public static RedissonClient newRedisson(String addresses) {
        Config config = new Config();
        config.setUseScriptCache(true);
        config.useClusterServers().addNodeAddress(addresses.split(","));
        return Redisson.create(config);
    }

}
