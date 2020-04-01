package com.xiaohongshu.db.hercules.rdbms.schema;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.exceptions.SchemaException;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.rdbms.common.options.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RDBMSSchemaFetcher extends BaseSchemaFetcher<Integer> {

    private static final Log LOG = LogFactory.getLog(RDBMSSchemaFetcher.class);

    private static final int YEAR_TYPE = Types.SMALLINT;

    private RDBMSManager manager;

    /**
     * 根据options拼出来的最终查询sql（对结果的全量查询sql，不带split的where条件）
     */
    private String querySql;

    private List<String> columnNameList;
    private Map<String, DataType> columnTypeMap;

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

    /**
     * 将select * 替换成具体列名
     */
    private void updateQuerySql() {
        if (!getOptions().hasProperty(RDBMSInputOptionsConf.QUERY)
                && !getOptions().hasProperty(BaseDataSourceOptionsConf.COLUMN)) {
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
        columnTypeMap = new HashMap<>();
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            statement = manager.getConnection().createStatement();
            resultSet = statement.executeQuery(getMetaDataSql());
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
                columnNameList.add(colName);
                // mysql bug: http://bugs.mysql.com/bug.php?id=35115
                // 在显式指定（yearIsDateType=false）year类型不为date类型后，jdbc仍然会把year类型当作date类型
                if (type == Types.DATE && StringUtils.equalsIgnoreCase(metadata.getColumnTypeName(i), "year")) {
                    type = YEAR_TYPE;
                }
                columnTypeMap.put(colName, convertType(type));
            }

            // 检查列数是否和配置的column一致
            if (getOptions().hasProperty(BaseDataSourceOptionsConf.COLUMN)) {
                int confSize = getOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null).length;
                if (confSize != columnNameList.size()) {
                    throw new SchemaException(String
                            .format("Unmatch column num between conf and actual situation: %d vs %d",
                                    confSize, columnNameList.size()));
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

    public RDBMSSchemaFetcher(GenericOptions options) {
        super(options);
        manager = new RDBMSManager(getOptions());
        initQuerySql();
        initMetaData();
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
    protected DataType convertType(Integer standard) {
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
    protected Map<String, DataType> innerGetColumnTypeMap() {
        return columnTypeMap;
    }

    public String getQuerySql() {
        return querySql;
    }

    public RDBMSManager getManager() {
        return manager;
    }

    public String getPrimaryKey() {
        throw new UnsupportedOperationException(String.format("Unsupported to get primary key in %s.",
                getClass().getCanonicalName()));
    }
}
