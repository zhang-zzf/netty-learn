package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Setter
@Getter
@Accessors(chain = true)
public class ConnAck extends ControlPacket {

    public static final byte ACCEPTED = 0x00;
    public static final byte UNACCEPTED_PROTOCOL_VERSION = 0x01;
    public static final byte IDENTIFIER_REJECTED = 0x02;
    public static final byte SERVER_UNAVAILABLE = 0x03;
    public static final byte NOT_AUTHORIZED = 0x05;

    private boolean sp;
    private int returnCode;

    public ConnAck() {
        this(false, 0x00);
    }

    public ConnAck(int returnCode) {
        this(false, returnCode);
    }

    public ConnAck(boolean sp, int returnCode) {
        // If a server sends a CONNACK packet containing a non-zero return code
        // it MUST set Session Present to 0
        if (returnCode != 0) {
            sp = false;
        }
        this.sp = sp;
        this.returnCode = returnCode;
        ByteBuf buf = Unpooled.buffer(4);
        buf.writeByte(0x20);
        buf.writeByte(0x02);
        buf.writeByte(sp ? 0x01 : 0x00);
        buf.writeByte(returnCode);
        this.setBuf(buf);
    }

}
