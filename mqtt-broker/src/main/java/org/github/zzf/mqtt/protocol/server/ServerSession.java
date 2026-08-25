package org.github.zzf.mqtt.protocol.server;

import io.netty.channel.ChannelFuture;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.session.Session;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/06/23
 */
public interface ServerSession extends Session {

    ChannelFuture forward(String clientId, String topicFilter, Publish packet);

}
