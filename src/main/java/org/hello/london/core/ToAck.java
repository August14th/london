package org.hello.london.core;

import java.util.Date;

class ToAck {
    public ToAck(int msgid, String topic, Date sendTime) {
        this.msgid = msgid;
        this.topic = topic;
        this.sendTime = sendTime;
    }

    public int msgid;

    public String topic;

    public Date sendTime;
}
