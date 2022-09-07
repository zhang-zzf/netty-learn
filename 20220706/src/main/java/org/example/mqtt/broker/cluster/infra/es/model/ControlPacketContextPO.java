package org.example.mqtt.broker.cluster.infra.es.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * use "${clientIdentifier}_${packetIdentifier}" as ID
 * <pre>
 * PUT /session_queue
 * {
 *   "settings": {
 *     "index.number_of_shards": 3,
 *     "index.number_of_replicas": 1,
 *     "refresh_interval": "10m"
 *   },
 *   "mappings": {
 *     "dynamic": "false",
 *     "properties": {
 *       "id": {
 *         "type": "keyword"
 *       },
 *       "clientIdentifier": {
 *         "type": "keyword"
 *       },
 *       "type": {
 *         "type": "keyword"
 *       },
 *       "packetIdentifier": {
 *         "type": "short"
 *       },
 *       "status": {
 *         "type": "keyword"
 *       },
 *       "publish": {
 *         "type": "binary"
 *       },
 *       "createdAt": {
 *         "type": "date"
 *       },
 *       "updatedAt": {
 *         "type": "date"
 *       },
 *       "nextPacketIdentifier": {
 *         "type": "short"
 *       }
 *     }
 *   }
 * }
 * </pre>
 */
@Data
@Accessors(chain = true)
public class ControlPacketContextPO {

    private String id;
    private String clientIdentifier;
    private String type;
    private Short packetIdentifier;
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
    private Long createdAt;
    private Long updatedAt;
    /**
     * pointer that point to the next Node
     * <p>null - has no next Node</p>
      */
    private Short nextPacketIdentifier;

}
