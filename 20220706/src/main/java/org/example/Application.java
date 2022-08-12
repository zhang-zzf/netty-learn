package org.example;

import com.alibaba.fastjson.JSON;
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
        log.info("Application#main: {}", JSON.toJSONString(args));
        boolean clientMode = Boolean.getBoolean("mqtt.client.mode");
        if (clientMode) {
            ClientBootstrap.main(args);
        } else {
            BrokerBootstrap.main(args);
        }
        // 启动 MicroMeter 打点框架

    }

}
