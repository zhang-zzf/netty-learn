package org.example;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.example.config.micrometer.MicroMeterConfiguration;
import org.example.mqtt.broker.node.bootstrap.BrokerBootstrap;
import org.example.mqtt.broker.metrics.BrokerBootstrapWithMetrics;
import org.example.mqtt.client.bootstrap.ClientBootstrap;

import javax.net.ssl.SSLException;

/**
 * @author zhang.zzf
 * @date 2020-04-18
 */
@Slf4j
public class Application {

    public static void main(String[] args) throws SSLException {
        log.info("Application#main: {}", JSON.toJSONString(args));
        boolean clientMode = Boolean.getBoolean("mqtt.client.mode");
        if (clientMode) {
            new Thread(() -> ClientBootstrap.main(args), "bootstrap-thread").start();
        } else {
            boolean metricsServer = Boolean.getBoolean("mqtt.server.metrics");
            if (metricsServer) {
                BrokerBootstrapWithMetrics.main(args);
            } else {
                BrokerBootstrap.main(args);
            }
        }
        // 启动 MicroMeter 打点框架
        new MicroMeterConfiguration().init("20220706");
    }

}
