package com.xiaohongshu.db.hercules.rdbms.schema.manager;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class RDBMSManager {

    private static final Log LOG = LogFactory.getLog(RDBMSManager.class);

    protected GenericOptions options;

    public RDBMSManager(@NonNull GenericOptions options) {
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

        String username = options.getString(RDBMSOptionsConf.USERNAME, null);
        String password = options.getString(RDBMSOptionsConf.PASSWORD, null);
        String connectString = options.getString(RDBMSOptionsConf.CONNECTION, null);

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

        String username = options.getString(RDBMSOptionsConf.USERNAME, null);
        String password = options.getString(RDBMSOptionsConf.PASSWORD, null);
        String connectString = options.getString(RDBMSOptionsConf.CONNECTION, null);

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

    public boolean execute(String sql) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        Integer fetchSize = options.getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);
        try {
            connection = getConnection();
            statement = SqlUtils.makeReadStatement(connection, sql, fetchSize);
            LOG.debug("Executing SQL statement: " + sql);
            return statement.execute();
        } finally {
            SqlUtils.release(Lists.newArrayList(statement, connection));
        }
    }

    public <T> List<T> executeSelect(String sql, int seq, ResultSetGetter<T> resultSetGetter) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        Integer fetchSize = options.getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);
        try {
            connection = getConnection();
            statement = SqlUtils.makeReadStatement(connection, sql, fetchSize);
            LOG.debug("Executing SQL statement: " + sql);
            return SqlUtils.resultSetToList(statement.executeQuery(), seq, resultSetGetter, true);
        } finally {
            SqlUtils.release(Lists.newArrayList(statement, connection));
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

    private String makeQueryByParts(String tableName, List<String> columnNameList, String condition) {
        return String.format("SELECT %s FROM %s %s",
                columnNameList.stream().map(SqlUtils::encloseColumnName).collect(Collectors.joining(", ")),
                tableName,
                condition);
    }

    public String makeBaseQuery() {
        return makeBaseQuery(false);
    }

    /**
     * @param useAsterisk 使用星号去查询，当且仅当schema fetcher获取全部列时为true，在其他情况下column必已被定义好
     * @return
     */
    public String makeBaseQuery(boolean useAsterisk) {
        if (options.hasProperty(RDBMSInputOptionsConf.QUERY)) {
            return options.getString(RDBMSInputOptionsConf.QUERY, null);
        } else {
            List<String> columnNameList;
            if (!options.hasProperty(TableOptionsConf.COLUMN)) {
                if (useAsterisk) {
                    columnNameList = Lists.newArrayList("*");
                } else {
                    throw new RuntimeException();
                }
            } else {
                columnNameList
                        = Arrays.asList(options.getTrimmedStringArray(TableOptionsConf.COLUMN, null));
            }
            String where = options.hasProperty(RDBMSInputOptionsConf.CONDITION)
                    ? " WHERE " + options.getString(RDBMSInputOptionsConf.CONDITION, null)
                    : "";
            String table = options.getString(RDBMSOptionsConf.TABLE, null);
            return makeQueryByParts(table, columnNameList, where);
        }
    }
}
