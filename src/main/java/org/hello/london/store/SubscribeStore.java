package org.hello.london.store;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class SubscribeStore {

    private DataSource postgres;

    public SubscribeStore(DataSource postgres) {
        this.postgres = postgres;
    }

    public List<String> get(String userId) throws Exception {
        try (Connection conn = postgres.getConnection()) {
            try (PreparedStatement select = conn.prepareStatement("SELECT topicid FROM subscribes WHERE userid = ?")) {
                select.setString(1, userId);
                try (ResultSet rs = select.executeQuery()) {
                    List<String> topics = new ArrayList<>();
                    while (rs.next()) {
                        topics.add(rs.getString(1));
                    }
                    return topics;
                }
            }
        }
    }

    public List<MessagesRange> getOfflineMessageRanges(String userId) throws Exception {
        Connection conn = postgres.getConnection();
        List<MessagesRange> range = new ArrayList<>();
        try (PreparedStatement select = conn.prepareStatement("SELECT sub.topicid, sub.lastackid, t.lastmsgid FROM subscribes sub JOIN topics t " +
                "ON sub.topicid = t.id WHERE sub.userid = ? AND sub.lastackid != t.lastmsgid")) {
            select.setString(1, userId);
            try (ResultSet rs = select.executeQuery()) {
                while (rs.next()) {
                    range.add(new MessagesRange(rs.getString(1), rs.getLong(2), rs.getLong(3)));
                }
                return range;
            }
        } finally {
            conn.close();
        }
    }

    public void updateLastAckId(String userId, String topic, long lastAckId) throws Exception {
        try (Connection conn = postgres.getConnection()) {
            try (PreparedStatement update = conn.prepareStatement("UPDATE subscribes SET lastackid = ? WHERE topicid = ? AND userid = ?")) {
                update.setLong(1, lastAckId);
                update.setString(2, topic);
                update.setString(3, userId);
                update.executeUpdate();
            }
        }
    }

    public void sub(String userid, List<String> topics) throws Exception {
        Connection conn = this.postgres.getConnection();
        conn.setAutoCommit(false);
        try {
            for (String topic : topics) {
                boolean subscribed = false;
                // 1 query whether subscribed or not
                try (PreparedStatement query = conn.prepareStatement("SELECT 1 FROM subscribes WHERE topicid = ? AND userid = ?")) {
                    query.setString(1, topic);
                    query.setString(2, userid);
                    ResultSet rs = query.executeQuery();
                    if (rs.next()) {
                        subscribed = true;
                    }
                    rs.close();
                }
                if (!subscribed) {
                    // 2 get last message's id
                    long lastMsgId = 0;
                    try (PreparedStatement select = conn.prepareStatement("SELECT lastmsgid FROM topics WHERE id = ? ")) {
                        select.setString(1, topic);
                        try (ResultSet rs = select.executeQuery()) {
                            if (rs.next()) {
                                lastMsgId = rs.getInt(1);
                            }
                        }
                    }
                    // 3 subscribe
                    try (PreparedStatement insert = conn.prepareStatement("INSERT INTO subscribes(topicid, userid, lastackid) VALUES(?, ?, ?)")) {
                        insert.setString(1, topic);
                        insert.setString(2, userid);
                        insert.setLong(3, lastMsgId);
                        insert.executeUpdate();
                    }
                }
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw new RuntimeException("Subscribe failed.", e);
        } finally {
            conn.close();
        }
    }

    public void unSub(String userId, List<String> topics) throws Exception {
        Connection conn = this.postgres.getConnection();
        conn.setAutoCommit(false);
        try {
            for (String topic : topics) {
                try (PreparedStatement delete = conn.prepareStatement("DELETE FROM subscribes WHERE topicid = ? AND userid = ?")) {
                    delete.setString(1, topic);
                    delete.setString(2, userId);
                    delete.executeUpdate();
                }
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw new RuntimeException("Delete subscribe failed", e);
        } finally {
            conn.close();
        }
    }
}
