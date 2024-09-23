package org.example.server.echo_group.echo;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/02
 */
@Slf4j
@ChannelHandler.Sharable
public class EchoChannelHandler extends SimpleChannelInboundHandler<String> {

    private static final List<Channel> CHANNELS = new ArrayList<>();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        log.info("channelRead0: {}", msg);
        // broadcast
        for (Channel channel : CHANNELS) {
            if (channel.equals(ctx.channel())) {
                continue;
            }
            channel.writeAndFlush(msg);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("channelActive: {}", ctx);
        CHANNELS.add(ctx.channel());
        super.channelActive(ctx);
        ctx.fireChannelActive();
    }

}
