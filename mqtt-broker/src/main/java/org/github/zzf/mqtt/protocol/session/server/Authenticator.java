package org.github.zzf.mqtt.protocol.session.server;

import org.github.zzf.mqtt.protocol.model.Connect;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/06/28
 */
public interface Authenticator {

    int AUTHENTICATE_SUCCESS = 0x00;

    /**
     * @return 0x00 authenticate success;
     * 0x02 Connection Refused, identifier rejected;
     * 0x04 Connection Refused, bad username or password
     */
    int authenticate(Connect packet);

}
