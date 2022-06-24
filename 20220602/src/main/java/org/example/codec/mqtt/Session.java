package org.example.codec.mqtt;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/23
 */
public interface Session {

    /**
     * a mqtt client that bind to the session
     *
     * @return client
     */
    Client client();

    /**
     * persistent
     *
     * @return true / false
     */
    boolean persistent();

    /**
     * send a message to client
     *
     * @param message message
     */
    void send(Message message);

    /**
     * receive a message from client
     *
     * @param message message
     */
    void receive(Message message);

}
