/*
 * Copyright (C) 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import cn.beecp.BeeDataSource;
import cn.beecp.BeeDataSourceConfig;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.apache.tomcat.jdbc.pool.Validator;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.vibur.dbcp.ViburDBCPDataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


@State(Scope.Benchmark)
public class BenchBase
{
    protected static final int MIN_POOL_SIZE = 20;
    @Param({ "hikari", "tomcat", "vibur", "bee" })
    public String pool;

    @Param({ "20" })
    public int maxPoolSize;

    //@Param({ "oracle.jdbc.driver.OracleDriver" })
    @Param({"com.mysql.jdbc.Driver"})
    public String driverClassName;

    //@Param({ "ORASTORE" })
    @Param({ "pm" }) //STG Mysql
//    @Param({ "ids" }) //QA Mysql
    public String username;

    //@Param({ "bisakzpt14(" }) //STG
    //@Param({ "qasp*34!" }) //QA
    @Param({"!Pm.pgps2#"}) // STG MySql
//    @Param({ "ids" }) //QA Mysql
    public String password;

    //@Param({ "jdbc:oracle:thin@172.19.55.70:1521/SP" }) //STG
//    @Param({ "jdbc:oracle:thin:@172.21.196.150:1521/STGSP" }) //QA
    @Param({ "jdbc:mysql://172.21.236.111:3306/ids" }) //STG Mysql
//    @Param({ "jdbc:mysql://172.21.236.79:3306/ids" }) //QA Mysql
    public String jdbcUrl;

    public static DataSource DS;

    @Setup(Level.Trial)
    public void setup(BenchmarkParams params)
    {
        try
        {
            Class.forName(driverClassName);
            System.err.printf("Using driver (%s): %s", jdbcUrl, DriverManager.getDriver(jdbcUrl));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        if (this.getClass().getName().contains("Statement")) {
            System.err.println("# Overriding maxPoolSize parameter for StatementBench: maxPoolSize=" + params.getThreads());
            maxPoolSize = params.getThreads();
        }

        switch (pool)
        {
        case "hikari":
            setupHikari();
            break;
        case "tomcat":
            setupTomcat();
            break;
        case "bee":
            setupBee();
            break;
        case "vibur":
            setupVibur();
            break;
        }

    }

    @TearDown(Level.Trial)
    public void teardown() throws SQLException
    {
        switch (pool)
        {
        case "hikari":
            ((HikariDataSource) DS).close();
            break;
        case "tomcat":
            ((org.apache.tomcat.jdbc.pool.DataSource) DS).close();
            break;
        case "bee":
            ((BeeDataSource) DS).close();
            break;
        case "vibur":
            ((ViburDBCPDataSource) DS).terminate();
            break;
        }
    }


    protected void setupTomcat()
    {
        PoolProperties props = new PoolProperties();
        props.setUrl(jdbcUrl);
        props.setDriverClassName(driverClassName);
        props.setUsername(username);
        props.setPassword(password);
        props.setInitialSize(MIN_POOL_SIZE);
        props.setMinIdle(MIN_POOL_SIZE);
        props.setMaxIdle(maxPoolSize);
        props.setMaxActive(maxPoolSize);
        props.setMaxWait(8000);

        props.setDefaultAutoCommit(false);

        props.setRollbackOnReturn(true);
        props.setUseDisposableConnectionFacade(true);
        props.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState"); //;org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
        props.setTestOnBorrow(true);
        props.setValidationInterval(1000);
        props.setValidator(new Validator() {
            @Override
            public boolean validate(Connection connection, int validateAction)
            {
                try {
                    return (validateAction == PooledConnection.VALIDATE_BORROW ? connection.isValid(0) : true);
                }
                catch (SQLException e)
                {
                    return false;
                }
            }
        });

        DS = new org.apache.tomcat.jdbc.pool.DataSource(props);
    }

    protected void setupHikari()
    {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverClassName);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(MIN_POOL_SIZE);
        config.setMaximumPoolSize(maxPoolSize);
        config.setConnectionTimeout(8000);
        config.setAutoCommit(false);

        DS = new HikariDataSource(config);
    }
    protected void setupBee()
    {
        BeeDataSourceConfig config = new BeeDataSourceConfig();
        config.setDriverClassName(driverClassName);
        config.setUsername(username);
        config.setPassword(password);
        config.setJdbcUrl(jdbcUrl);

        config.setInitialSize(MIN_POOL_SIZE); // min pool size
        config.setMaxActive(maxPoolSize);
        config.setMaxWait(8000);
        config.setDefaultAutoCommit(false);
        System.out.println("======================");
        if(config.getBorrowConcurrentSize() > config.getMaxActive()){
            config.setBorrowConcurrentSize(config.getMaxActive());
        }
        if(config.getInitialSize() > config.getMaxActive()){
            config.setInitialSize(config.getMaxActive());
        }

        System.out.println(config.getMaxActive());
        System.out.println(config.getBorrowConcurrentSize());
        DS = new BeeDataSource(config);
    }
    private void setupVibur()
    {
        ViburDBCPDataSource vibur = new ViburDBCPDataSource();
        vibur.setDriverClassName(driverClassName);
        vibur.setJdbcUrl( jdbcUrl );
        vibur.setUsername(username);
        vibur.setPassword(password);
        vibur.setConnectionTimeoutInMs(5000);
        vibur.setValidateTimeoutInSeconds(3);
        vibur.setLoginTimeoutInSeconds(2);
        vibur.setPoolInitialSize(MIN_POOL_SIZE);
        vibur.setPoolMaxSize(maxPoolSize);
        vibur.setConnectionIdleLimitInSeconds(1);
        vibur.setAcquireRetryAttempts(0);
        vibur.setReducerTimeIntervalInSeconds(0);
        vibur.setUseNetworkTimeout(true);
        vibur.setNetworkTimeoutExecutor(Executors.newFixedThreadPool(1));
        vibur.setClearSQLWarnings(true);
        vibur.setResetDefaultsAfterUse(true);
        if(vibur.getPoolInitialSize() > vibur.getPoolMaxSize()){
            vibur.setPoolInitialSize(vibur.getPoolMaxSize());
        }


        vibur.start();

        DS = vibur;
    }

}
