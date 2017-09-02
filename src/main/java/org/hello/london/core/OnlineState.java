package org.hello.london.core;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

public class OnlineState {

    private ConcurrentHashMap<String, Date> onlines = new ConcurrentHashMap<>();

    public void enter(String userid) {
        this.onlines.put(userid, new Date());
    }

    public void exit(String userid) {
        this.onlines.remove(userid);
    }
}
