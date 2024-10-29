package org.example.server.echo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/02
 */
@Slf4j
public class EchoChannelHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        log.info("utf8: {}", buf.toString(StandardCharsets.UTF_8));
        log.info("hex:\n{}", ByteBufUtil.prettyHexDump(buf));
        ByteBuf _16kBytes = ctx.alloc().directBuffer(16 * 1024);
        _16kBytes.writeZero(_16kBytes.writableBytes());
        ctx.write(_16kBytes);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
    }
}
