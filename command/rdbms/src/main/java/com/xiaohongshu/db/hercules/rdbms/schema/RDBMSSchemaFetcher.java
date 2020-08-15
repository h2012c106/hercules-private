package com.xiaohongshu.db.hercules.rdbms.schema;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.BaseDataSourceOptionsConf.COLUMN_TYPE;

public class RDBMSSchemaFetcher extends BaseSchemaFetcher {

    private static final Log LOG = LogFactory.getLog(RDBMSSchemaFetcher.class);

    private static final int YEAR_TYPE = Types.SMALLINT;

    @GeneralAssembly
    private RDBMSManager manager;

    @GeneralAssembly
    private RDBMSDataTypeConverter converter;

    protected final String baseSql;

    public RDBMSSchemaFetcher(GenericOptions options) {
        super(options);
        this.baseSql = SqlUtils.makeBaseQuery(options, true);
    }

    protected String getNoneLineSql(String baseSql) {
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

    private Map<String, DataType> getColumnTypeMap(String sql) {
        // 只是为了获得列名，只要拿key即可
        Set<String> configuredTypeNameSet = getOptions().getJson(COLUMN_TYPE, null).keySet();

        final Map<String, Integer> sqlTypeMap = new HashMap<>();
        getSchemaInfo(sql, new BiFunction<String, Integer, Void>() {
            @Override
            public Void apply(String s, Integer integer) {
                // 此处key为数据库里拿出来的列名
                sqlTypeMap.put(s, integer);
                return null;
            }
        });

        // 列名优先大小写不敏感地使用用户指定的，若用户没指定，再用数据库返回的，若不做这个处理且正好有冲突，则在合并map的时候两个都会存在，不合适，且有潜在风险
        return sqlTypeMap.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> {
                    String configuredName = findCaseInsensitiveInCollection(configuredTypeNameSet, entry.getKey());
                    return configuredName == null ? entry.getKey() : configuredName;
                }, entry -> converter.convertElementType(entry.getValue())));
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap() {
        Map<String, DataType> res = new HashMap<>();
        // load一遍sql，做这步的目的是为了load到例如a+b这种列的类型，若*的话这列就不会有
        res.putAll(getColumnTypeMap(baseSql));
        // 再load一遍所有的列，这步的目的是能够load到split-by或者update-key列
        res.putAll(getColumnTypeMap(SqlUtils.replaceSelectItem(baseSql, "*")));
        return res;
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

}
