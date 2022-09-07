package org.example.mqtt.broker.cluster.infra.es.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * use "${clientIdentifier}_${packetIdentifier}" as ID
 */
@Data
@Accessors(chain = true)
public class ControlPacketContextPO {

    private String id;
    private String clientIdentifier;
    private String type;
    private short packetIdentifier;
    /**
     * <p>{@link org.example.mqtt.session.ControlPacketContext.Status#INIT}</p>
     * <p>{@link org.example.mqtt.session.ControlPacketContext.Status#PUB_REC}</p>
     * <p>COMPLETE / DELETE / CANCEL</p>
     */
    private String status;
    /**
     * Base64.encode(new Publish().toByteBuf().array())
     */
    private String publish;
    private long createdAt;
    private long updatedAt;
    /**
     * pointer that point to the next Node
      */
    private short nextPacketIdentifier;

}
