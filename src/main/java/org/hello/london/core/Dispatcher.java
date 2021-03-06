package org.hello.london.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Dispatcher implements Runnable {

    private HashMap<String, Set<MqttHandler>> handlers = new HashMap<>();

    private PGConnection postgres;

    public Dispatcher(Connection conn) throws Exception {
        postgres = (PGConnection) conn;
        Statement stmt = conn.createStatement();
        stmt.execute("LISTEN london");
        stmt.close();
    }

    public void run() {
        try {
            while (true) {
                PGNotification[] notices = postgres.getNotifications(0);
                if (notices != null) {
                    for (PGNotification notice : notices) {
                        ObjectMapper mapper = new ObjectMapper();
                        Message msg = mapper.readValue(notice.getParameter(), Message.class);
                        synchronized (this) {
                            Set<MqttHandler> set = handlers.get(msg.topic);
                            if (set != null) {
                                for (MqttHandler handler : set) {
                                    handler.send(msg);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Dispatch failed.", e);
        } finally {
            try {
                ((Connection) postgres).close();
            } catch (Exception e) {
            }
        }
    }

    public synchronized void register(List<String> topics, MqttHandler handler) {
        if (topics == null) return;
        for (String topic : topics) {
            Set<MqttHandler> set = handlers.get(topic);
            if (set == null) {
                set = new HashSet<>();
                set.add(handler);
                handlers.put(topic, set);
            } else {
                set.add(handler);
            }
        }
    }

    public synchronized void deregister(List<String> topics, MqttHandler handler) {
        if (topics == null) return;
        for (String topic : topics) {
            Set<MqttHandler> set = handlers.get(topic);
            if(set != null){
                set.remove(handler);
            }
        }
    }
}
