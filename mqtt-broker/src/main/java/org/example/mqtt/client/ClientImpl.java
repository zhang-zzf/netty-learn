package org.example.mqtt.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-13
 */
@Slf4j
public class ClientImpl extends AbstractClient {

    /**
     * create a new Client
     *
     * @param clientIdentifier clientIdentifier
     * @param remoteAddress    mqtt://host:port
     * @param eventLoopGroup   group to use
     */
    public ClientImpl(String clientIdentifier, String remoteAddress, EventLoopGroup eventLoopGroup) throws URISyntaxException {
        super(clientIdentifier, new URI(remoteAddress), eventLoopGroup);
    }

}
