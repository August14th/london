package org.hello.london.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class Subscribes {

    private DataSource postgres;

    public Subscribes(DataSource postgres) {
        this.postgres = postgres;
    }

    public List<String> get(String userid) throws Exception {
        Connection conn = postgres.getConnection();
        try {
            PreparedStatement select = conn.prepareStatement("SELECT topicid FROM subscribes WHERE userid = ?");
            try {
                select.setString(1, userid);
                ResultSet rs = select.executeQuery();
                List<String> topics = new ArrayList<>();
                while (rs.next()) {
                    topics.add(rs.getString(1));
                }
                rs.close();
                return topics;
            } finally {
                select.close();
            }
        } finally {
            conn.close();
        }
    }

    public void updateLastAckId(String userId, String topic, long lastAckId) throws Exception {
        Connection conn = postgres.getConnection();
        try {
            PreparedStatement update = conn.prepareStatement("UPDATE subscribes SET lastackid = ? WHERE topicid = ? and userid = ?");
            try {
                update.setLong(1, lastAckId);
                update.setString(2, topic);
                update.setString(3, userId);
                update.executeUpdate();
            } finally {
                update.close();
            }
        } finally {
            conn.close();
        }
    }

    public void sub(String userid, List<String> topics) throws Exception {
        Connection conn = this.postgres.getConnection();
        conn.setAutoCommit(false);
        try {
            for (String topic : topics) {
                boolean subscribed = false;
                // 1 query whether subscribed or not
                PreparedStatement query = conn.prepareStatement("SELECT 1 FROM subscribes WHERE topicid = ? and userid = ?");
                try {
                    query.setString(1, topic);
                    query.setString(2, userid);
                    ResultSet rs = query.executeQuery();
                    if (rs.next()) {
                        subscribed = true;
                    }
                    rs.close();
                } finally {
                    query.close();
                }
                if (!subscribed) {
                    // 2 get last message's id
                    long lastMsgId = 0;
                    PreparedStatement select = conn.prepareStatement("SELECT lastmsgid FROM topics WHERE id = ? ");
                    try {
                        select.setString(1, topic);
                        ResultSet rs = select.executeQuery();
                        if (rs.next()) {
                            lastMsgId = rs.getInt(1);
                        }
                        rs.close();
                    } finally {
                        select.close();
                    }
                    // 3 subscribe
                    PreparedStatement insert = conn.prepareStatement("INSERT INTO subscribes(topicid, userid, lastackid) VALUES(?, ?, ?)");
                    try {
                        insert.setString(1, topic);
                        insert.setString(2, userid);
                        insert.setLong(3, lastMsgId);
                        insert.executeUpdate();
                    } finally {
                        insert.close();
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

    public void unSub(String userid, List<String> topics) throws Exception {
        Connection conn = this.postgres.getConnection();
        conn.setAutoCommit(false);
        try {
            for (String topic : topics) {
                PreparedStatement delete = conn.prepareStatement("DELETE FROM subscribes WHERE topicid = ? and userid = ?");
                delete.setString(1, topic);
                delete.setString(2, userid);
                delete.executeUpdate();
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
