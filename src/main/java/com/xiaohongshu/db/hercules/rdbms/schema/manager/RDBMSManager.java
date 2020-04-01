package com.xiaohongshu.db.hercules.rdbms.schema.manager;

import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.common.options.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import lombok.NonNull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RDBMSManager {

    private static final Log LOG = LogFactory.getLog(RDBMSManager.class);

    protected GenericOptions options;

    public RDBMSManager(GenericOptions options) {
        this.options = options;
    }

    /**
     * 子类可以设置如果获取不到时的默认值，如mysql等
     *
     * @return
     */
    protected String getDriverString() {
        String driverString = options.getString(RDBMSOptionsConf.DRIVER, null);
        if (driverString == null) {
            throw new UnsupportedOperationException(String.format("Please use '--%s' to specify the jdbc driver class.",
                    RDBMSOptionsConf.DRIVER));
        } else {
            return driverString;
        }
    }

    public Connection getConnection() throws SQLException {
        Connection connection;
        String driverClass = getDriverString();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: "
                    + driverClass);
        }

        String username = options.getString(RDBMSInputOptionsConf.USERNAME, null);
        String password = options.getString(RDBMSInputOptionsConf.PASSWORD, null);
        String connectString = options.getString(RDBMSInputOptionsConf.CONNECTION, null);

        if (username == null) {
            connection = DriverManager.getConnection(connectString);
        } else {
            connection = DriverManager.getConnection(
                    connectString, username, password);
        }

        return connection;
    }

    /**
     * 主要为了万一某种数据源需要加一些默认链接参数，如mysql为了fetch size的useCursorFetch，
     * 那么需要重写{@link #getConnection()}方法，在里面简单地调这个方法即可
     *
     * @param properties
     * @return
     * @throws SQLException
     */
    protected final Connection getConnection(Properties properties) throws SQLException {
        String driverClass = getDriverString();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: "
                    + driverClass);
        }

        String username = options.getString(RDBMSInputOptionsConf.USERNAME, null);
        String password = options.getString(RDBMSInputOptionsConf.PASSWORD, null);
        String connectString = options.getString(RDBMSInputOptionsConf.CONNECTION, null);

        Properties props = new Properties();
        if (username != null) {
            props.put("user", username);
        }

        if (password != null) {
            props.put("password", password);
        }

        props.putAll(properties);
        return DriverManager.getConnection(connectString, props);
    }


    /**
     * 由于迭代器特性，且会关闭资源，故是一次性的，再想执行需要重新生成resultSet
     *
     * @param in
     * @param seq
     * @param resultSetGetter
     * @param <T>
     * @return
     * @throws SQLException
     */
    private <T> List<T> resultSetToList(@NonNull ResultSet in, int seq, ResultSetGetter<T> resultSetGetter)
            throws SQLException {
        List<T> out = new ArrayList<>();
        try {
            while (in.next()) {
                // 不可能出现null
                out.add(resultSetGetter.get(in, seq));
            }
            return out;
        } finally {
            try {
                in.close();
            } catch (SQLException e) {
                LOG.warn("SQLException closing resultset: " + ExceptionUtils.getStackTrace(e));
            }
        }
    }

    public <T> List<T> executeSelect(String sql, int seq, ResultSetGetter<T> resultSetGetter) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        Integer fetchSize = options.getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);
        try {
            connection = getConnection();
            statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (fetchSize != null) {
                LOG.debug("Using fetchSize for next query: " + fetchSize);
                statement.setFetchSize(fetchSize);
            }
            LOG.debug("Executing SQL statement: " + sql);
            return resultSetToList(statement.executeQuery(), seq, resultSetGetter);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing statement: " + ExceptionUtils.getStackTrace(e));
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing connection: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    public String getRandomFunc() {
        String randFunc = options.getString(RDBMSInputOptionsConf.RANDOM_FUNC_NAME, null);
        if (randFunc == null) {
            throw new UnsupportedOperationException(String.format("If you are using balance mode, " +
                            "please use '--%s' to specify the random function name to enable random sampling.",
                    RDBMSInputOptionsConf.RANDOM_FUNC_NAME));
        } else {
            return randFunc;
        }
    }

}
