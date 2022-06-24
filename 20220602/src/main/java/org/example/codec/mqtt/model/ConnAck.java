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

    private boolean sp;
    private int returnCode;

    public ConnAck() {
        this(false, 0x00);
    }

    public ConnAck(int returnCode) {
        this(false, returnCode);
    }

    public ConnAck(boolean sp, int returnCode) {
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
