package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PublishInbound extends Publish implements ReferenceCounted {

    private final ByteBuf incoming;

    /**
     * inbound packet convert to model
     *
     * @param incoming inbound packet
     */
    public PublishInbound(ByteBuf incoming) {
        super(incoming);
        /**
         PublishInbound is a subclass of ReferencedCounted
         todo test
         todo retainedSlice zero-copy
         watch out: memory leak
         this.payload use in inbound case will be release by netty.
         the PublishInbound is a ReferenceCounted,
         so the payload will be released by the {@link io.netty.channel.DefaultChannelPipeline.TailContext.channelRead}
         */
        this.incoming = incoming.retain();
    }

    @Override
    public int refCnt() {
        return incoming.refCnt();
    }

    @Override
    public PublishInbound retain() {
        return retain(1);
    }

    @Override
    public PublishInbound retain(int i) {
        incoming.retain(i);
        return this;
    }

    @Override
    public PublishInbound touch() {
        return this;
    }

    @Override
    public PublishInbound touch(Object o) {
        return this;
    }

    @Override
    public boolean release() {
        return release(1);
    }

    @Override
    public boolean release(int i) {
        return incoming.release(i);
    }

}
