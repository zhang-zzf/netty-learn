package org.github.zzf.mqtt.mqtt.client;

import org.github.zzf.mqtt.protocol.model.Publish;

public interface MessageHandler {

    void handle(String topic, Publish packet);

    void clientClosed();

}
