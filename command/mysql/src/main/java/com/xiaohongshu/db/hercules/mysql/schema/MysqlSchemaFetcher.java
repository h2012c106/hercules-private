package com.xiaohongshu.db.hercules.mysql.schema;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
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

    @Override
    protected List<Set<String>> innerGetIndexGroupList() {
        if (getOptions().hasProperty(QUERY)) {
            LOG.warn("Unable to fetch index info with a sql.");
            return super.innerGetIndexGroupList();
        } else {
            return getKeyInfo(getOptions().getString(TABLE, null), false);
        }
    }

    @Override
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        if (getOptions().hasProperty(QUERY)) {
            LOG.warn("Unable to fetch unique key info with a sql.");
            return super.innerGetUniqueKeyGroupList();
        } else {
            return getKeyInfo(getOptions().getString(TABLE, null), true);
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
