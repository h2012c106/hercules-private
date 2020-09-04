package com.xiaohongshu.db.hercules.mysql.schema;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.QUERY;
import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf.TABLE;

public class MysqlSchemaFetcher extends RDBMSSchemaFetcher {

    private static final Log LOG = LogFactory.getLog(MysqlSchemaFetcher.class);

    @GeneralAssembly
    private RDBMSManager manager;

    public MysqlSchemaFetcher(GenericOptions options) {
        super(options);
    }

    private boolean caseIgnoredIn(String item, Collection<String> c) {
        for (String cItem : c) {
            if (StringUtils.equalsIgnoreCase(item, cItem)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        List<String> superRes = super.innerGetColumnNameList();
        if (getOptions().getOptionsType().isSource()) {
            return superRes;
        } else {
            // 为下游时，generated列不得被插入
            String tableName = getOptions().getString(TABLE, null);
            String sql = String.format("SHOW CREATE TABLE `%s`;", tableName);
            String createTable;
            try {
                createTable = manager.executeSelect(sql, 2, ResultSetGetter.STRING_GETTER).get(0);
            } catch (SQLException e) {
                LOG.warn(String.format("Failed to fetch table [%s] generated column info, treated as a normal table, exception: %s.", tableName, e.getMessage()));
                return superRes;
            }
            List<String> generatedColumn = new MySqlCreateTableParser(createTable)
                    .parseCreateTable()
                    .getTableElementList()
                    .stream()
                    .filter(ele -> ele instanceof SQLColumnDefinition)
                    .map(ele -> (SQLColumnDefinition) ele)
                    .filter(ele -> ele.getGeneratedAlawsAs() != null)
                    .map(ele -> SqlUtils.unwrapBacktick(ele.getColumnName()))
                    .collect(Collectors.toList());
            List<String> res = new LinkedList<>();
            // 保持大小写一致
            List<String> generatedSameCaseColumn = new LinkedList<>();
            for (String column : superRes) {
                if (!caseIgnoredIn(column, generatedColumn)) {
                    res.add(column);
                } else {
                    generatedSameCaseColumn.add(column);
                }
            }
            if (generatedSameCaseColumn.size() > 0) {
                LOG.info("Fetch generated column, remove from target column list, removed column list is: " + generatedSameCaseColumn);
            }
            return res;
        }
    }

    private List<Set<String>> getKeyInfo(String tableName, boolean unique) {
        String sql = String.format("SHOW KEYS FROM `%s`;", tableName);
        ResultSet resultSet = null;
        Statement statement = null;
        Connection connection = null;
        Map<String, Map<Integer, String>> keyMap = new HashMap<>();
        try {
            connection = manager.getConnection();
            statement = connection.createStatement();
            LOG.info("Execute sql to fetch table key info: " + sql);
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                // unique时只需要取unique的
                if (unique && resultSet.getBoolean("Non_unique")) {
                    continue;
                }
                String keyName = resultSet.getString("Key_name");
                String columnName = resultSet.getString("Column_name");
                Integer columnSeq = resultSet.getInt("Seq_in_index");
                keyMap.computeIfAbsent(keyName, key -> new HashMap<>()).put(columnSeq, columnName);
            }
        } catch (SQLException e) {
            throw new SchemaException(e);
        } finally {
            SqlUtils.release(Lists.newArrayList(resultSet, statement, connection));
        }
        // unique时不需要按照最左原则拆开
        if (unique) {
            return keyMap.values().stream().map(map -> new HashSet<>(map.values())).collect(Collectors.toList());
        } else {
            // 把各个key group按照key内列顺序排序
            List<Set<String>> res = new LinkedList<>();
            for (Map<Integer, String> rowKeyGroup : keyMap.values()) {
                res.addAll(
                        SchemaUtils.unwrapIndexList(
                                rowKeyGroup.entrySet()
                                        .stream()
                                        .sorted(new Comparator<Map.Entry<Integer, String>>() {
                                            @Override
                                            public int compare(Map.Entry<Integer, String> o1, Map.Entry<Integer, String> o2) {
                                                return o1.getKey().compareTo(o2.getKey());
                                            }
                                        })
                                        .map(Map.Entry::getValue)
                                        .collect(Collectors.toList())
                        )
                );
            }
            return res;
        }
    }

    private String fetchTableNameFromQuery(String sql) {
        // 从sql里把表名撸出来
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statement.accept(visitor);
        Map<TableStat.Name, ?> tableStats = visitor.getTables();
        if (tableStats.size() != 1) {
            LOG.info(String.format("Cannot fetch table name from the sql [%s].", sql));
            return null;
        }
        String table = tableStats.keySet().iterator().next().getName();
        try {
            manager.execute("DESC " + table + ";");
            LOG.info(String.format("The sql [%s]'s table is: %s", sql, table));
            return table;
        } catch (SQLException e) {
            LOG.info(String.format("Cannot fetch table name from the sql [%s], exception: %s.", sql, e.getMessage()));
            return null;
        }
    }

    @Override
    protected List<Set<String>> innerGetIndexGroupList() {
        String tableName;
        if (getOptions().hasProperty(QUERY)) {
            tableName = fetchTableNameFromQuery(getOptions().getString(QUERY, null));
            if (tableName == null) {
                LOG.warn("Unable to fetch index info with sql.");
                return super.innerGetIndexGroupList();
            }
        } else {
            tableName = getOptions().getString(TABLE, null);
        }
        return getKeyInfo(tableName, false);
    }

    @Override
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        String tableName;
        if (getOptions().hasProperty(QUERY)) {
            tableName = fetchTableNameFromQuery(getOptions().getString(QUERY, null));
            if (tableName == null) {
                LOG.warn("Unable to fetch unique key info with sql.");
                return super.innerGetUniqueKeyGroupList();
            }
        } else {
            tableName = getOptions().getString(TABLE, null);
        }
        return getKeyInfo(tableName, true);
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
