package org.example.mqtt.broker;

import java.util.Set;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public interface Topic {

    String topic();

    Set<Subscription> subscription();

}
