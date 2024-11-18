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
        this.incoming = incoming;
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
