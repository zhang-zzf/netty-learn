package org.example.mqtt.broker;

import org.example.mqtt.model.Connect;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
public interface Authenticator {

    int AUTHENTICATE_SUCCESS = 0x00;

    /**
     * @return 0x00 authenticate success;
     * 0x02 Connection Refused, identifier rejected;
     * 0x04 Connection Refused, bad user name or password
     */
    int authenticate(Connect packet);

}
