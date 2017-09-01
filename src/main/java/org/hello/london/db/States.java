package org.hello.london.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Calendar;

public class States {

    private DataSource postgres;

    public States(DataSource postgres) {
        this.postgres = postgres;
    }

    public void enter(String userid, Calendar time) throws Exception {
        Connection conn = postgres.getConnection();
        boolean found;
        try {
            conn.setAutoCommit(false);
            PreparedStatement update = conn.prepareStatement("UPDATE states SET entertime = ?, exittime = null WHERE userid = ?");
            try {
                update.setTimestamp(1, new Timestamp(time.getTimeInMillis()));
                update.setString(2, userid);
                found = update.executeUpdate() != 0;
            } finally {
                update.close();
            }
            if (!found) {
                PreparedStatement insert = conn.prepareStatement("INSERT INTO states(userid, entertime, exittime) VALUES (?, ?, null)");
                try {
                    insert.setString(1, userid);
                    insert.setTimestamp(2, new Timestamp(time.getTimeInMillis()));
                    insert.executeUpdate();
                } finally {
                    insert.close();
                }
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw new RuntimeException("Update entertime failed.", e);
        } finally {
            conn.close();
        }
    }

    public void exit(String userid, Calendar time) throws Exception {
        Connection conn = postgres.getConnection();
        try {
            PreparedStatement update = conn.prepareStatement("UPDATE states SET exittime = ? WHERE userid = ?");
            try {
                update.setTimestamp(1, new Timestamp(time.getTimeInMillis()));
                update.setString(2, userid);
                update.executeUpdate();
            } finally {
                update.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Update exittime failed.", e);
        } finally {
            conn.close();
        }
    }
}
