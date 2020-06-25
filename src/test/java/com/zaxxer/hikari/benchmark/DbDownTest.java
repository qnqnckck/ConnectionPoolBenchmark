package com.zaxxer.hikari.benchmark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPDataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DbDownTest
{
    private static final String JDBC_URL = "jdbc:mysql://172.21.236.79/ids";

    private static final Logger LOGGER = LoggerFactory.getLogger(DbDownTest.class);

    private static final int MIN_POOL_SIZE = 5;
    private int maxPoolSize = MIN_POOL_SIZE;

    private DataSource hikariDS;
    private DataSource viburDS;

    public static void main(String[] args)
    {
        DbDownTest dbDownTest = new DbDownTest();
        dbDownTest.start();
    }

    private DbDownTest()
    {
        hikariDS = setupHikari();
        viburDS = setupVibur();
    }

    private void start()
    {
        class MyTask extends TimerTask
        {
            private DataSource ds;
            public ResultSet resultSet;

            MyTask(DataSource ds)
            {
                this.ds = ds;
            }

            @Override
            public void run()
            {
                try (Connection c = ds.getConnection()) {
                    LOGGER.info("{} got a connection.", ds.getClass().getSimpleName(), c);
                    try (Statement stmt = c.createStatement()) {
                        LOGGER.debug("{} Statement ({})", ds.getClass().getSimpleName(), System.identityHashCode(stmt));
                        stmt.setQueryTimeout(1);
                        resultSet = stmt.executeQuery("SELECT uid FROM tb_access");
                        if (resultSet.next()) {
                            LOGGER.debug("Ran query got {}", resultSet.getInt(1));
                        }
                        else {
                            LOGGER.warn("{} Query executed, got no results.", ds.getClass().getSimpleName());
                        }
                    }
                    catch (SQLException e) {
                        LOGGER.error("{} Exception executing query, got a bad connection from the pool: {}", ds.getClass().getSimpleName(), e.getMessage());
                    }
                }
                catch (Throwable t)
                {
                    LOGGER.error("{} Exception getting connection: {}", ds.getClass().getSimpleName(), t.getMessage());
                }
            }
        }

        new Timer(true).schedule(new MyTask(hikariDS), 5000, 2000);
        new Timer(true).schedule(new MyTask(viburDS), 5000, 2000);

        try
        {
            Thread.sleep(TimeUnit.SECONDS.toMillis(300));
        }
        catch (InterruptedException e)
        {
            return;
        }
    }

    protected DataSource setupHikari()
    {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        // config.setDriverClassName("com.mysql.jdbc.Driver");
        config.setUsername("ids");
        config.setPassword("ids");
        config.setConnectionTimeout(5000);
        config.setMinimumIdle(MIN_POOL_SIZE);
        config.setMaximumPoolSize(maxPoolSize);
        config.setInitializationFailTimeout(0L);
        config.setConnectionTestQuery("SELECT 1");

        return new HikariDataSource(config);
    }

    private DataSource setupVibur()
    {
        ViburDBCPDataSource vibur = new ViburDBCPDataSource();
        vibur.setJdbcUrl( JDBC_URL );
        vibur.setUsername("ids");
        vibur.setPassword("ids");
        vibur.setConnectionTimeoutInMs(5000);
        vibur.setValidateTimeoutInSeconds(3);
        vibur.setLoginTimeoutInSeconds(2);
        vibur.setPoolInitialSize(MIN_POOL_SIZE);
        vibur.setPoolMaxSize(maxPoolSize);
        vibur.setConnectionIdleLimitInSeconds(1);
        vibur.setAcquireRetryAttempts(0);
        vibur.setReducerTimeIntervalInSeconds(0);
        vibur.setUseNetworkTimeout(true);
        vibur.setNetworkTimeoutExecutor(Executors.newCachedThreadPool());
        vibur.setClearSQLWarnings(true);
        vibur.setResetDefaultsAfterUse(true);
        vibur.setTestConnectionQuery("isValid"); // this is the default option, can be left commented out
        vibur.start();
        return vibur;
    }
}
