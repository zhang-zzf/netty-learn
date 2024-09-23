package org.example.mqtt.client;

import org.example.mqtt.model.Publish;

public interface MessageHandler {

    void handle(String topic, Publish packet);

    void clientClosed();

}
