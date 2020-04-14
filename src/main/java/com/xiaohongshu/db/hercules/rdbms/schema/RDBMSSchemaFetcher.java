package com.xiaohongshu.db.hercules.rdbms.schema;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.StingyMap;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RDBMSSchemaFetcher extends BaseSchemaFetcher<Integer> {

    private static final Log LOG = LogFactory.getLog(RDBMSSchemaFetcher.class);

    private static final int YEAR_TYPE = Types.SMALLINT;

    protected RDBMSManager manager;

    /**
     * 根据options拼出来的最终查询sql（对结果的全量查询sql，不带split的where条件）
     */
    private String querySql;

    private List<String> columnNameList;
    /**
     * 如果是以table指定的这个map里存的应当不止columnNameList里的列，应当是所有的列，
     * 因为对下游而言，事实上split-by以及update-key并不需要包含在columnNameList里，但是同样需要类型信息
     */
    private StingyMap<String, DataType> columnTypeMap;

    private StingyMap<String, Integer> columnSqlTypeMap;

    private void initQuerySql() {
        if (getOptions().hasProperty(RDBMSInputOptionsConf.QUERY)) {
            querySql = getOptions().getString(RDBMSInputOptionsConf.QUERY, null);
        } else {
            String column = getOptions().hasProperty(BaseDataSourceOptionsConf.COLUMN)
                    ? String.join(", ", getOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null))
                    : "*";
            String where = getOptions().hasProperty(RDBMSInputOptionsConf.CONDITION)
                    ? " WHERE " + getOptions().getString(RDBMSInputOptionsConf.CONDITION, null)
                    : "";
            querySql = String.format("SELECT %s FROM %s %s",
                    column,
                    getOptions().getString(RDBMSOptionsConf.TABLE, null),
                    where);
        }
    }

    private void updateQuerySql() {
        if (!getOptions().hasProperty(RDBMSInputOptionsConf.QUERY)) {
            String column = String.join(", ", getColumnNameList());
            String where = getOptions().hasProperty(RDBMSInputOptionsConf.CONDITION)
                    ? " WHERE " + getOptions().getString(RDBMSInputOptionsConf.CONDITION, null)
                    : "";
            querySql = String.format("SELECT %s FROM %s %s",
                    column,
                    getOptions().getString(RDBMSOptionsConf.TABLE, null),
                    where);
        }
    }

    private void initMetaData() {
        columnNameList = new ArrayList<>();
        columnSqlTypeMap = new StingyMap<>();
        ResultSet resultSet = null;
        Statement statement = null;
        String[] configuredColumnNameList = getOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null);
        try {
            statement = manager.getConnection().createStatement();
            String sql = getMetaDataSql();
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
                    throw new SchemaException(String.format("Unable to parse #%d column name in query: %s",
                            i, querySql));
                }

                // mysql bug: http://bugs.mysql.com/bug.php?id=35115
                // 在显式指定（yearIsDateType=false）year类型不为date类型后，jdbc仍然会把year类型当作date类型
                if (type == Types.DATE && StringUtils.equalsIgnoreCase(metadata.getColumnTypeName(i), "year")) {
                    type = YEAR_TYPE;
                }

                // query模式时不可相信此时拿到的colName，因为columns和query里的列只是下标对应，万一sql里没写as就翻车了
                if (getOptions().hasProperty(RDBMSInputOptionsConf.QUERY)) {
                    colName = configuredColumnNameList[i - 1];
                    columnNameList.add(colName);
                    columnSqlTypeMap.put(colName, type);
                } else {
                    // 这里如果有column配置的名字尽量去用，不然会导致明明用户输入了小写，但是从这里出去变成了大写，这对column map影响很大
                    if (configuredColumnNameList != null) {
                        colName = configuredColumnNameList[i - 1];
                    }
                    columnNameList.add(colName);
                    columnSqlTypeMap.put(colName, type);
                }
            }
        } catch (SQLException e) {
            throw new SchemaException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing resultset: " + ExceptionUtils.getStackTrace(e));
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing statement: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    private String getAdditionalColumnMetaDataSql(List<String> additionalColumnList) {
        String sql = getMetaDataSql();
        return SqlUtils.replaceSelectItem(sql, additionalColumnList.stream()
                .map(SqlUtils::makeItem)
                .toArray(SQLSelectItem[]::new));
    }

    private void updateColumnTypeMap(List<String> additionalColumnList) {
        ResultSet resultSet = null;
        Statement statement = null;
        String sql = getAdditionalColumnMetaDataSql(additionalColumnList);
        try {
            statement = manager.getConnection().createStatement();
            LOG.info("Execute sql to fetch additional column type: " + sql);
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metadata = resultSet.getMetaData();
            for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                int type = metadata.getColumnType(i);
                String colName = additionalColumnList.get(i - 1);

                // mysql bug: http://bugs.mysql.com/bug.php?id=35115
                // 在显式指定（yearIsDateType=false）year类型不为date类型后，jdbc仍然会把year类型当作date类型
                if (type == Types.DATE && StringUtils.equalsIgnoreCase(metadata.getColumnTypeName(i), "year")) {
                    type = YEAR_TYPE;
                }

                columnSqlTypeMap.put(colName, type);
            }
        } catch (SQLException e) {
            throw new SchemaException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing resultset: " + ExceptionUtils.getStackTrace(e));
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing statement: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    protected RDBMSManager setManager() {
        return new RDBMSManager(getOptions());
    }

    public RDBMSSchemaFetcher(GenericOptions options) {
        super(options);
        manager = setManager();
        initQuerySql();
        initMetaData();

        // 上面只登记了column的列，但是没有登记update-key或split-by，只要是TABLE模式就得来这么一发，就算不填columns默认全上也有可能出现大小写问题
        List<String> additionalColumnList = new ArrayList<>();
        if (options.hasProperty(RDBMSInputOptionsConf.SPLIT_BY)) {
            additionalColumnList.add(options.getString(RDBMSInputOptionsConf.SPLIT_BY, null));
        }
        if (options.hasProperty(RDBMSOutputOptionsConf.UPDATE_KEY)) {
            additionalColumnList.addAll(Arrays.asList(options.getStringArray(RDBMSOutputOptionsConf.UPDATE_KEY, null)));
        }
        if (additionalColumnList.size() > 0) {
            updateColumnTypeMap(additionalColumnList);
        }

        updateQuerySql();
    }


    @Override
    public DataSource getDataSource() {
        return DataSource.RDBMS;
    }

    protected String getMetaDataSql() {
        return SqlUtils.addWhere(querySql, "1 = 0");
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        return columnNameList;
    }

    @Override
    public DataType convertType(Integer standard) {
        switch (standard) {
            case Types.NULL:
                return DataType.NULL;
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return DataType.INTEGER;
            case Types.BIT:
            case Types.BOOLEAN:
                return DataType.BOOLEAN;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DataType.DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return DataType.STRING;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return DataType.DATE;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return DataType.BYTES;
            default:
                throw new SchemaException("Unsupported sql type, type code: " + standard);
        }
    }

    @Override
    protected StingyMap<String, DataType> innerGetColumnTypeMap() {
        columnTypeMap = new StingyMap<>();
        for(Map.Entry<String,Integer> entry:columnSqlTypeMap.entrySet()){
            columnTypeMap.put(entry.getKey(),convertType(entry.getValue()));
        }
        return columnTypeMap;
    }

    public StingyMap<String, Integer> getColumnSqlTypeMap() {
        return columnSqlTypeMap;
    }

    public String getQuerySql() {
        return querySql;
    }

    public RDBMSManager getManager() {
        return manager;
    }

    public String getPrimaryKey() throws SQLException {
        if (getOptions().hasProperty(RDBMSOptionsConf.TABLE)) {
            Connection connection = getManager().getConnection();
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String schemaName = connection.getSchema();
            String tableName = getOptions().getString(RDBMSOptionsConf.TABLE, null);
            ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, schemaName, tableName);
            int i = 0;
            String res = null;
            while (resultSet.next()) {
                res = resultSet.getString(4);
                ++i;
            }
            if (i == 0) {
                return null;
            } else if (i > 1) {
                throw new SchemaException(String.format("There are %d table found with the name of [%s.%s], " +
                                "please manually specify the split key.",
                        i, schemaName, tableName));
            } else {
                // 万一pk不在columns里，那么需要同样地做split-by的更新type map操作
                updateColumnTypeMap(Lists.newArrayList(res));
                return res;
            }
        } else {
            throw new UnsupportedOperationException("Query type input must clarify the split key.");
        }
    }
}
