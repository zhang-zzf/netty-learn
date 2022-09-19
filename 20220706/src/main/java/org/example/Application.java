package org.example;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.example.config.micrometer.MicroMeterConfiguration;
import org.example.mqtt.broker.metrics.BrokerBootstrapWithMetrics;
import org.example.mqtt.broker.node.bootstrap.BrokerBootstrap;
import org.example.mqtt.client.bootstrap.ClientBootstrap;

import javax.net.ssl.SSLException;
import java.net.URISyntaxException;

/**
 * @author zhang.zzf
 * @date 2020-04-18
 */
@Slf4j
public class Application {

    public static void main(String[] args) throws URISyntaxException, SSLException {
        log.info("Application#main: {}", JSON.toJSONString(args));
        if (Boolean.getBoolean("mqtt.server.cluster.enable")) {
            // Server cluster mode
            org.example.mqtt.broker.cluster.bootstrap.BrokerBootstrap.main(args);
            return;
        }
        if (Boolean.getBoolean("mqtt.server.metrics")) {
            // Server metric mode (just for Node mode)
            BrokerBootstrapWithMetrics.main(args);
            // 启动 MicroMeter 打点框架
            new MicroMeterConfiguration().init("20220706");
            return;
        }
        if (Boolean.getBoolean("mqtt.client.mode")) {
            new Thread(() -> ClientBootstrap.main(args), "bootstrap-thread").start();
            return;
        }
        // Server Node mode
        BrokerBootstrap.main(args);
    }

}
