package org.example.mqtt.client;

import org.example.mqtt.model.Publish;

public interface MessageHandler {

    void handle(String topic, byte[] payload, Publish packet);

    void clientClosed();

}
