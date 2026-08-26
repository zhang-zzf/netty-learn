package org.github.zzf.mqtt.protocol.model;

import static java.util.Collections.unmodifiableList;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;

public class Unsubscribe extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xA2;
    final short packetIdentifier;
    final List<String> topicFilters;

    private Unsubscribe(byte _0Byte,
            int remainingLength,
            short packetIdentifier,
            List<String> topicFilters) {
        super(_0Byte, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.topicFilters = topicFilters;
    }

    public static Unsubscribe incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        short packetIdentifier = readPacketIdentifier(incoming);
        List<String> topicFilters = new ArrayList<>(4);
        while (incoming.isReadable()) {
            topicFilters.add(readUTF8String(incoming));
        }
        return new Unsubscribe(byte0, remainingLength,
                packetIdentifier,
                unmodifiableList(topicFilters));
    }

    public List<Subscribe.Subscription> subscriptions() {
        return topicFilters.stream()
                .map(topicFilter -> new Subscription(topicFilter, (byte) 0x0))
                .toList();
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeTwoByteInteger(buf, packetIdentifier);
        for (String tf : topicFilters) {
            writeUTF8String(buf, tf);
        }
        return buf;
    }

    private ByteBuf toPacketByteBuf() {
        return super.toByteBuf();
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public boolean packetValidate() {
        // Bits 3,2,1 and 0 of the fixed header of the UNSUBSCRIBE Control Packet are reserved and MUST be set to
        // 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection
        if (this.byte0 != _0_BYTE) {
            return false;
        }
        //  The payload of a UNSUBSCRIBE packet MUST contain at least one Topic Filter.
        return topicFilters != null && !topicFilters.isEmpty();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":").append(hexPId(packetIdentifier)).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public static class V50 extends Unsubscribe {
        // If there are no properties, this MUST be indicated by including a Property Length of zero
        final Properties properties;
        final Set<Integer> allowedProperties = Set.of(USER_PROPERTY);

        V50(byte byte0, int remainingLength,
                short packetIdentifier, Properties properties,
                List<String> topicFilters) {
            super(byte0, remainingLength, packetIdentifier, topicFilters);
            this.properties = properties;
        }

        public static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            short packetIdentifier = readPacketIdentifier(incoming);
            Properties properties = readProperties(incoming);
            List<String> topicFilters = new ArrayList<>(4);
            while (incoming.isReadable()) {
                topicFilters.add(readUTF8String(incoming));
            }
            return new V50(byte0, remainingLength,
                    packetIdentifier, properties,
                    unmodifiableList(topicFilters));
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toPacketByteBuf();
            writeTwoByteInteger(buf, packetIdentifier);
            writeProperties(buf, properties);
            for (String tf : topicFilters) {
                writeUTF8String(buf, tf);
            }
            return buf;
        }

        public Properties properties() {
            return this.properties;
        }

        @Override
        public boolean packetValidate() {
            return super.packetValidate()
                    && properties.validateIdentifier(allowedProperties);
        }

        @Override
        public List<Subscribe.Subscription> subscriptions() {
            return topicFilters.stream()
                    .map(topicFilter -> new Subscription.V50(topicFilter, (byte) 0x0, null))
                    .map(d -> (Subscription) d)
                    .toList();
        }

    }


}
