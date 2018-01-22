package org.hello.london.store;

public class MessagesRange {

    public MessagesRange(String topic, long start, long end) {
        this.topic = topic;
        this.start = start;
        this.end = end;
    }

    public String topic;

    public long start;

    public long end;
}
