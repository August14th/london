package org.hello.london.db;

public class OfflineMessagesMeta {

    public OfflineMessagesMeta(String topic, long start, long end) {
        this.topic = topic;
        this.start = start;
        this.end = end;
    }

    public String topic;

    public long start;

    public long end;
}
