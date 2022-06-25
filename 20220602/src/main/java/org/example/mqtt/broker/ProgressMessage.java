package org.example.mqtt.broker;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public interface ProgressMessage extends Message {

    int STATUS_INIT = 1 << 0;
    int STATUS_SENT = 1 << 1;
    int STATUS_RELEASE = 1 << 2;
    int STATUS_COMPLETE = 3 << 3;

    /**
     * current
     */
    int progress();

    long lastProgressTime();

    void updateProgress(int status);


}
