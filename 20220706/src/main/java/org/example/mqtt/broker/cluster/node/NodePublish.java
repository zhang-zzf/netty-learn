package org.example.mqtt.broker.cluster.node;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import org.example.mqtt.model.Publish;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.example.mqtt.model.Publish.*;

/**
 * 性能考虑
 */
@Data
public class NodePublish extends NodeMessage {

    /**
     * meta
     */
    private long metaPReceive;
    /**
     * meta
     */
    private long metaNmWrap;
    /**
     * meta
     */
    private long metaNmReceive = System.currentTimeMillis();
    /**
     * the PublishPacket
     */
    private ByteBuf publishPacket;

    public NodePublish() {
    }

    public NodePublish(ByteBuf buf) {
        setPacket(ACTION_PUBLISH_FORWARD);
        setNodeId(buf.readCharSequence(buf.readShort(), UTF_8).toString());
        this.metaPReceive = buf.readLong();
        this.metaNmWrap = buf.readLong();
        // just a view of the origin buf
        // NO COPY
        this.publishPacket = buf.slice();
    }

    public final ByteBuf toByteBuf() {
        byte[] nodeIdBytes = getNodeId().getBytes(UTF_8);
        ByteBuf meta = Unpooled.buffer(2 + nodeIdBytes.length + 2 * 8);
        meta.writeShort(nodeIdBytes.length);
        meta.writeBytes(nodeIdBytes);
        meta.writeLong(metaPReceive);
        meta.writeLong(metaNmWrap);
        // create view of two ByteBuf.
        // NO COPY
        return Unpooled.compositeBuffer()
                .addComponent(true, meta)
                .addComponent(true, publishPacket);
    }

    public static NodePublish wrapPublish(String nodeId, Publish packet) {
        NodePublish nm = new NodePublish();
        nm.setNodeId(nodeId);
        nm.setPacket(ACTION_PUBLISH_FORWARD);
        nm.publishPacket = packet.toByteBuf();
        metaData(nm, packet);
        return nm;
    }

    private static void metaData(NodePublish nm, Publish packet) {
        if (packet.meta() != null) {
            nm.metaPReceive = (long) packet.meta().get(META_P_RECEIVE);
        }
        nm.metaNmWrap = System.currentTimeMillis();
    }

    public Publish unwrapPublish() {
        Publish publish = new Publish(publishPacket);
        // just for metric usage
        // transfer to Publish packet
        metricMeta(publish);
        return publish;
    }

    private void metricMeta(Publish publish) {
        publish.addMeta(META_P_RECEIVE, metaPReceive);
        publish.addMeta(META_NM_WRAP, metaNmWrap);
        publish.addMeta(META_NM_RECEIVE, metaNmReceive);
        // mark source
        publish.addMeta(META_P_SOURCE, META_P_SOURCE_BROKER);
    }

}