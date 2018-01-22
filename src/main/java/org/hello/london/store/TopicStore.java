package org.hello.london.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hello.london.core.Message;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TopicStore {

    private DataSource postgres;

    private MessageStore msgTable;

    public TopicStore(DataSource postgres, MessageStore msgTable) {
        this.postgres = postgres;
        this.msgTable = msgTable;
    }

    public Message publish(String topic, byte[] payload) throws Exception {
        Connection conn = postgres.getConnection();
        try {
            long lastMsgId = -1;
            conn.setAutoCommit(false);
            try (PreparedStatement update = conn.prepareStatement("UPDATE topics SET lastmsgid = lastmsgid + 1 WHERE id = ? RETURNING lastmsgid")) {
                // increase last message's id
                update.setString(1, topic);
                try (ResultSet rs = update.executeQuery()) {
                    if (rs.next()) {
                        lastMsgId = rs.getLong(1);
                    }
                }
            }
            if (lastMsgId == -1) {
                // create topic
                try (PreparedStatement insert = conn.prepareStatement("INSERT INTO topics(id, lastmsgid) VALUES (?, 1)")) {
                    insert.setString(1, topic);
                    boolean ok = insert.executeUpdate() == 1;
                    if (!ok) {
                        throw new SQLException("Create topic failed.");
                    }
                }
                lastMsgId = 1;
            }
            //persist to mongo
            Message msg = new Message(topic, lastMsgId, payload);
            try {
                this.msgTable.append(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // notify through postgres' channel        ;
            try (PreparedStatement notify = conn.prepareStatement("SELECT pg_notify(?, ?)")) {
                notify.setString(1, "london");
                ObjectMapper mapper = new ObjectMapper();
                notify.setString(2, mapper.writeValueAsString(msg));
                notify.executeQuery().close();
                conn.commit();
                return msg;
            }
        } catch (Exception e) {
            conn.rollback();
            throw new RuntimeException("Publish failed.", e);
        } finally {
            conn.close();
        }
    }
}