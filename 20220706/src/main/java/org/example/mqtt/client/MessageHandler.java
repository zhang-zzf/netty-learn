package org.example.mqtt.client;

public interface MessageHandler {

    void handle(String topic, byte[] payload);

    void clientClosed();

}
