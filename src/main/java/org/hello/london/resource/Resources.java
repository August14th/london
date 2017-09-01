package org.hello.london.resource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Resources {

    public DataSource postgres;

    public int port;

    public int maxIdleTime;

    public Resources() throws Exception {
        Conf conf = readConf();
        postgres = getDataSource(conf.postgres.c3p0);
        port = conf.port;
        maxIdleTime = conf.maxIdleTime;
    }

    private Conf readConf() throws Exception {
        InputStream is = Resources.class.getResourceAsStream("/conf.json");
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(is, Conf.class);
        } finally {
            try {
                is.close();
            } catch (IOException e) {

            }
        }
    }

    private DataSource getDataSource(C3P0 c3p0) throws Exception {
        ComboPooledDataSource ds = new ComboPooledDataSource(true);
        ds.setDataSourceName("C3PO");
        ds.setJdbcUrl(c3p0.jdbcUrl);
        ds.setDriverClass(c3p0.driverClass);
        ds.setUser(c3p0.user);
        ds.setPassword(c3p0.password);
        ds.setMaxIdleTime(c3p0.maxIdleTime);
        ds.setMaxPoolSize((c3p0.maxPoolSize));
        ds.setMinPoolSize(c3p0.minPoolSize);

        return ds;
    }
}

