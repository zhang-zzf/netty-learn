package org.github.zzf.mqtt.mqtt.broker.cluster.infra.redis.model;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import lombok.Data;
import lombok.experimental.Accessors;
import org.github.zzf.mqtt.mqtt.broker.cluster.ClusterControlPacketContext;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.session.ControlPacketContext;

import javax.annotation.Nullable;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

@Data
@Accessors(chain = true)
public class CpxPO {

    /**
     * packetIdentifier
     * <p>使用 hex 表示</p>
     */
    private String pId;
    /**
     * <p>{@link ControlPacketContext.Status#INIT}</p>
     * <p>{@link ControlPacketContext.Status#PUB_REC}</p>
     */
    private String status;
    /**
     * Base64.encode(new Publish().toByteBuf().array())
     */
    private String publish;
    /**
     * point to the next pId
     */
    private String next;

    public static CpxPO from(short packetIdentifier, ControlPacketContext.Status status) {
        return new CpxPO()
                .setPId(encodePacketIdentifier(packetIdentifier))
                .setStatus(encodeStatus(status));
    }

    public static CpxPO fromDomain(ClusterControlPacketContext cpx) {
        CpxPO po = new CpxPO()
                .setPId(encodePacketIdentifier(cpx.packetIdentifier()))
                .setStatus(encodeStatus(cpx.status()))
                .setPublish(encodePublish(cpx));
        return po;
    }

    public static List<CpxPO> jsonDecodeArray(String json) {
        return JSON.parseArray(json, CpxPO.class);
    }

    public static String encodeStatus(ControlPacketContext.Status status) {
        return status.name();
    }

    public ControlPacketContext.Status decodeStatus() {
        return decodeStatus(this);
    }

    public static ControlPacketContext.Status decodeStatus(CpxPO po) {
        return ControlPacketContext.Status.valueOf(po.getStatus());
    }

    public Publish decodePublish() {
        return decodePublish(this);
    }

    public static Publish decodePublish(CpxPO po) {
        return new Publish(Base64.decode(Unpooled.copiedBuffer(po.getPublish(), UTF_8)));
    }

    public static String encodePublish(ClusterControlPacketContext cpx) {
        return Base64.encode(cpx.packet().toByteBuf()).toString(UTF_8);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (pId != null) {
            sb.append("\"pId\":\"").append(pId).append("\",");
        }
        if (status != null) {
            sb.append("\"status\":\"").append(status).append('\"').append(',');
        }
        if (publish != null) {
            sb.append("\"publish\":\"").append(publish).append('\"').append(',');
        }
        if (next != null) {
            sb.append("\"next\":\"").append(next).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String jsonEncode() {
        return JSON.toJSONString(this);
    }

    public static CpxPO jsonDecode(String json) {
        return JSON.parseObject(json, CpxPO.class);
    }

    public static String encodePacketIdentifier(Short packetIdentifier) {
        return ControlPacket.hexPId(packetIdentifier);
    }

    @Nullable
    public static Short decodePacketIdentifier(String pId) {
        if (pId == null) {
            return null;
        }
        return ControlPacket.hexPIdToShort(pId);
    }

    @Nullable
    public Short decodeNextPacketIdentifier() {
        return decodePacketIdentifier(next);
    }

}
