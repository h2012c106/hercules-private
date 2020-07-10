package com.xiaohongshu.db.hercules.rdbms.schema;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class RDBMSSchemaFetcher extends BaseSchemaFetcher<RDBMSDataTypeConverter> {

    private static final Log LOG = LogFactory.getLog(RDBMSSchemaFetcher.class);

    private static final int YEAR_TYPE = Types.SMALLINT;

    private RDBMSManager manager;
    private String baseSql;

    public RDBMSSchemaFetcher(GenericOptions options,
                              RDBMSDataTypeConverter converter,
                              RDBMSManager manager) {
        super(options, converter);
        this.manager = manager;
        this.baseSql = SqlUtils.makeBaseQuery(options, true);
    }

    public RDBMSSchemaFetcher(GenericOptions options,
                              RDBMSManager manager) {
        this(options, new RDBMSDataTypeConverter(), manager);
    }


    private String getNoneLineSql(String baseSql) {
        return SqlUtils.addWhere(baseSql, "1 = 0");
    }

    private void getSchemaInfo(String baseSql, BiFunction<String, Integer, Void> dealWithColumnSchemaFunc) {
        String sql = getNoneLineSql(baseSql);
        ResultSet resultSet = null;
        Statement statement = null;
        Connection connection = null;
        try {
            connection = manager.getConnection();
            statement = connection.createStatement();
            LOG.info("Execute sql to fetch column and column type: " + sql);
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metadata = resultSet.getMetaData();
            for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                int type = metadata.getColumnType(i);
                String colName = metadata.getColumnLabel(i);
                if (colName == null || "".equals(colName)) {
                    colName = metadata.getColumnName(i);
                }
                if (colName == null || "".equals(colName)) {
                    throw new SchemaException(String.format("Unable to parse #%d column name in query: %s", i, sql));
                }

                // mysql bug: http://bugs.mysql.com/bug.php?id=35115
                // 在显式指定（yearIsDateType=false）year类型不为date类型后，jdbc仍然会把year类型当作date类型
                if (type == Types.DATE && StringUtils.equalsIgnoreCase(metadata.getColumnTypeName(i), "year")) {
                    type = YEAR_TYPE;
                }

                dealWithColumnSchemaFunc.apply(colName, type);
            }
        } catch (SQLException e) {
            throw new SchemaException(e);
        } finally {
            SqlUtils.release(Lists.newArrayList(resultSet, statement, connection));
        }
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        final List<String> res = new ArrayList<>();
        getSchemaInfo(baseSql, new BiFunction<String, Integer, Void>() {
            @Override
            public Void apply(String s, Integer integer) {
                res.add(s);
                return null;
            }
        });
        return res;
    }

    private String findCaseInsensitiveInCollection(Collection<String> collection, String s) {
        for (String item : collection) {
            if (StringUtils.equalsIgnoreCase(item, s)) {
                return item;
            }
        }
        return null;
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap(final Set<String> columnNameSet) {
        final Map<String, Integer> res = new HashMap<>();
        // 用嵌套查询，避免使用sql模式时select到不存在(as出来)的列
        String sql = SqlUtils.replaceSelectItem(String.format("SELECT * from ( %s ) as t;", baseSql), columnNameSet.toArray(new String[0]));
        getSchemaInfo(sql, new BiFunction<String, Integer, Void>() {
            @Override
            public Void apply(String s, Integer integer) {
                String originalColumnName = findCaseInsensitiveInCollection(columnNameSet, s);
                // 取列的原名，而不是用数据库返回的名字（大小写问题）
                if (originalColumnName != null) {
                    res.put(originalColumnName, integer);
                }
                return null;
            }
        });
        return res.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> converter.convertElementType(entry.getValue())));
    }

    @Override
    protected Set<String> getAdditionalNeedTypeColumn() {
        Set<String> res = new HashSet<>();
        if (getOptions().hasProperty(RDBMSInputOptionsConf.SPLIT_BY)) {
            res.add(getOptions().getString(RDBMSInputOptionsConf.SPLIT_BY, null));
        } else {
            try {
                String pk = getPrimaryKey();
                if (pk != null) {
                    res.add(pk);
                }
            } catch (SQLException e) {
                LOG.warn("Exception occurs during fetch primary key: " + ExceptionUtils.getStackTrace(e));
            }
        }
        if (getOptions().hasProperty(RDBMSOutputOptionsConf.UPDATE_KEY)) {
            res.addAll(Arrays.asList(getOptions().getStringArray(RDBMSOutputOptionsConf.UPDATE_KEY, null)));
        }
        return res;
    }

    /**
     * 在schema fetcher获得列的类型时，必拿一次，然后在getSplits时还可能会拿一次
     *
     * @return
     * @throws SQLException
     */
    public String getPrimaryKey() throws SQLException {
        if (getOptions().hasProperty(RDBMSOptionsConf.TABLE)) {
            Connection connection = null;
            ResultSet resultSet = null;
            try {
                connection = manager.getConnection();
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                String schemaName = connection.getSchema();
                String tableName = getOptions().getString(RDBMSOptionsConf.TABLE, null);
                resultSet = databaseMetaData.getPrimaryKeys(schemaName, schemaName, tableName);
                int i = 0;
                String res = null;
                while (resultSet.next()) {
                    res = resultSet.getString(4);
                    ++i;
                }
                if (i == 0) {
                    return null;
                } else if (i > 1) {
                    LOG.warn(String.format("There are %d table found with the name of [%s.%s], unable to fetch primary key.",
                            i, schemaName, tableName));
                    return null;
                } else {
                    return res;
                }
            } finally {
                SqlUtils.release(Lists.newArrayList(resultSet, connection));
            }
        } else {
            return null;
        }
    }

    public int getColumnSqlType(String baseSql, final String column) {
        String sql = SqlUtils.replaceSelectItem(baseSql, column);
        final Map<String, Integer> resMap = new HashMap<>(1);
        getSchemaInfo(sql, new BiFunction<String, Integer, Void>() {
            @Override
            public Void apply(String s, Integer integer) {
                if (StringUtils.equalsIgnoreCase(s, column)) {
                    resMap.put(column, integer);
                }
                return null;
            }
        });
        Integer res = resMap.get(column);
        if (res == null) {
            throw new SchemaException(String.format("Unable to fetch column [%s] sql type with sql: %s", column, baseSql));
        } else {
            return res;
        }
    }

    public boolean isIndex(String columnName) throws SQLException {
        if (getOptions().hasProperty(RDBMSOptionsConf.TABLE)) {
            Connection connection = null;
            ResultSet resultSet = null;
            try {
                connection = manager.getConnection();
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                String schemaName = connection.getSchema();
                String tableName = getOptions().getString(RDBMSOptionsConf.TABLE, null);
                resultSet = databaseMetaData.getIndexInfo(schemaName, schemaName, tableName, false, true);
                while (resultSet.next()) {
                    int order = resultSet.getInt("ORDINAL_POSITION");
                    String name = resultSet.getString("COLUMN_NAME");
                    if (StringUtils.equalsIgnoreCase(name, columnName)) {
                        if (order <= 1) {
                            return true;
                        } else {
                            LOG.error(String.format("Though column [%s] is the member of [%s], but it's not the first one.",
                                    name, resultSet.getString("INDEX_NAME")));
                            return false;
                        }
                    }
                }
                return false;
            } finally {
                SqlUtils.release(Lists.newArrayList(resultSet, connection));
            }
        } else {
            LOG.warn("Cannot get the index information from sql, ignore key check.");
            return true;
        }
    }

    public Set<String> getAutoincrementColumn() {
        String sql = getNoneLineSql(baseSql);
        ResultSet resultSet = null;
        Statement statement = null;
        Connection connection = null;
        Set<String> res = new HashSet<>(1);
        try {
            connection = manager.getConnection();
            statement = connection.createStatement();
            LOG.info("Execute sql to fetch column and column type: " + sql);
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metadata = resultSet.getMetaData();
            for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                if (metadata.isAutoIncrement(i)) {
                    res.add(metadata.getColumnName(i));
                }
            }
            LOG.info("Autoincrement columns: " + res);
            return res;
        } catch (SQLException e) {
            throw new SchemaException(e);
        } finally {
            SqlUtils.release(Lists.newArrayList(resultSet, statement, connection));
        }
    }
}
