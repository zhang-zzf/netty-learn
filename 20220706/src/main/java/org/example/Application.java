package org.example;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.BrokerBootstrap;
import org.example.mqtt.client.ClientBootstrap;

/**
 * @author zhang.zzf
 * @date 2020-04-18
 */
@Slf4j
public class Application {

    public static void main(String[] args) throws InterruptedException {
        log.info("Application#main: {}", args);
        boolean clientMode = Boolean.getBoolean(System.getProperty("mqtt.client.mode", "false"));
        if (clientMode) {
            ClientBootstrap.main(args);
        } else {
            BrokerBootstrap.main(args);
        }
    }

}
