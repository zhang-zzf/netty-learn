package org.example;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.config.MicroMeterConfiguration;
import org.example.bootstrap.ClusterBrokerBootstrap;
import org.example.bootstrap.ClusterDbRepoImplPressure;
import org.example.bootstrap.BrokerBootstrap;
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
        String appName = System.getProperty("appName", "mqtt-broker");
        log.info("appName: {}", appName);
        new MicroMeterConfiguration().init(appName);
        if (Boolean.getBoolean("mqtt.server.cluster.mode.pressure")) {
            ClusterDbRepoImplPressure.main(args);
            return;
        }
        if (Boolean.getBoolean("mqtt.server.cluster.enable")) {
            // Server cluster mode
            ClusterBrokerBootstrap.main(args);
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
