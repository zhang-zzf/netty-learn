package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_GRANTED_QOS_0;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_GRANTED_QOS_1;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_GRANTED_QOS_2;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_NOT_AUTHORIZED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PACKET_ID_IN_USE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_QUOTA_EXCEEDED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_TOPIC_FILTER_INVALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_UNSPECIFIED_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Set;

public class SubAck extends ControlPacket {

    public static final int REMAINING_LENGTH_FIELD_LENGTH = 2;
    final short packetIdentifier;
    final byte[] returnCodes;

    private SubAck(int remainingLength,
            short packetIdentifier,
            byte[] returnCodes) {
        super(SUBACK, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.returnCodes = returnCodes;
    }

    public static SubAck incoming(ByteBuf incoming) {
        readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        short packetIdentifier = readPacketIdentifier(incoming);
        byte[] returnCodes = readBytes(incoming, incoming.readableBytes());
        return new SubAck(remainingLength, packetIdentifier, returnCodes);
    }

    public static SubAck from(short packetIdentifier, List<Integer> reasonCodes) {
        int size = reasonCodes.size();
        byte[] returnCodes = new byte[size];
        for (int i = 0; i < size; i++) {
            returnCodes[i] = (byte) (reasonCodes.get(i) & 0xFF);
        }
        int remainingLength = REMAINING_LENGTH_FIELD_LENGTH + size;
        SubAck ret = new SubAck(remainingLength, packetIdentifier, returnCodes);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException();
        }
        return ret;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeTwoByteInteger(buf, packetIdentifier);
        writeBytes(buf, returnCodes);
        return buf;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":").append(hexPId(packetIdentifier)).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    private ByteBuf toPacketByteBuf() {
        return super.toByteBuf();
    }

    public static class V50 extends SubAck {

        // If there are no properties, this MUST be indicated by including a Property Length of zero
        final Properties properties;
        final Set<Integer> allowedProperties = Set.of(REASON_STRING, USER_PROPERTY);

        V50(int remainingLength,
                short packetIdentifier, Properties properties,
                byte[] reasonCodes) {
            super(remainingLength, packetIdentifier, reasonCodes);
            this.properties = properties;
        }

        public static V50 incoming(ByteBuf incoming) {
            readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            short packetIdentifier = readPacketIdentifier(incoming);
            Properties properties = readProperties(incoming);
            byte[] reasonCodes = readBytes(incoming, incoming.readableBytes());
            return new V50(remainingLength,
                    packetIdentifier, properties,
                    reasonCodes);
        }

        public static SubAck.V50 from(
                short packetIdentifier,
                List<Integer> reasonCodes,
                Properties properties) {
            int size = reasonCodes.size();
            byte[] returnCodes = new byte[size];
            for (int i = 0; i < size; i++) {
                returnCodes[i] = (byte) (reasonCodes.get(i) & 0xFF);
            }
            int remainingLength = REMAINING_LENGTH_FIELD_LENGTH
                    + calcPropertiesLength(properties)
                    + size ;
            SubAck.V50 ret = new SubAck.V50(remainingLength, packetIdentifier, properties, returnCodes);
            if (!ret.packetValidate()) {
                throw new MalformedPacketException();
            }
            return ret;
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toPacketByteBuf();
            writeTwoByteInteger(buf, packetIdentifier);
            writeProperties(buf, properties);
            writeBytes(buf, returnCodes);
            return buf;
        }

        public Properties properties() {
            return this.properties;
        }

        @Override
        public boolean packetValidate() {
            return super.packetValidate()
                    && properties.validateIdentifier(allowedProperties)
                    && validateReasonCodes();
        }

        private boolean validateReasonCodes() {
            if (returnCodes.length == 0) {
                return false;
            }
            for (byte returnCode : returnCodes) {
                if (!isValidReasonCode(returnCode)) {
                    return false;
                }
            }
            return true;
        }

        private boolean isValidReasonCode(byte returnCode) {
            return returnCode == REASON_CODE_GRANTED_QOS_0
                    || returnCode == REASON_CODE_GRANTED_QOS_1
                    || returnCode == REASON_CODE_GRANTED_QOS_2
                    || returnCode == REASON_CODE_UNSPECIFIED_ERROR
                    || returnCode == REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR
                    || returnCode == REASON_CODE_NOT_AUTHORIZED
                    || returnCode == REASON_CODE_PACKET_ID_IN_USE
                    || returnCode == REASON_CODE_QUOTA_EXCEEDED
                    || returnCode == REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED
                    || returnCode == REASON_CODE_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED
                    || returnCode == REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED
                    || returnCode == REASON_CODE_TOPIC_FILTER_INVALID
                    ;
        }

    }
}
