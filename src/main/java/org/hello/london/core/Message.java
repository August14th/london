package org.hello.london.core;

public class Message {

    public Message(){

    }

    public Message(String topic, long msgId, byte[] payload) {
        this.topic = topic;
        this.payload = payload;
        this.msgId = msgId;
    }

    public String topic;

    public byte[] payload;

    public long msgId;
}
