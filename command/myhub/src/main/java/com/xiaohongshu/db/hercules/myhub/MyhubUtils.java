package com.xiaohongshu.db.hercules.myhub;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.Map;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.QUERY;
import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf.TABLE;

public final class MyhubUtils {

    private static final Log LOG = LogFactory.getLog(MyhubUtils.class);

    /**
     * @return 0代表非shard表，否则为shard表shard数
     */
    public static int getShardNum(GenericOptions options, RDBMSManager manager) throws SQLException {
        String table;
        if (options.hasProperty(TABLE)) {
            table = options.getString(TABLE, null);
            table = "`" + table + "`";
        } else {
            // 从sql里把表名撸出来
            String sql = options.getString(QUERY, null);
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLStatement statement = parser.parseStatement();
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            statement.accept(visitor);
            Map<TableStat.Name, ?> tableStats = visitor.getTables();
            if (tableStats.size() != 1) {
                throw new SQLException(String.format("The sql [%s] contains more than one table, hercules cannot deal this sql on myhub: %s", sql, tableStats.keySet()));
            }
            table = tableStats.keySet().iterator().next().getName();
            LOG.info(String.format("The sql [%s]'s table is: %s", sql, table));
        }
        String shardInfoSql = String.format("show shard_info %s;", table);
        LOG.info("Execute sql to fetch myhub shard info: " + shardInfoSql);
        try {
            return manager.executeSelect(shardInfoSql, 3, ResultSetGetter.INT_GETTER).get(0);
        } catch (SQLException e) {
            // 目前只能通过catch到SQLException的error code来断言是否shard表，若将来不会抛错了，那这个逻辑要改。
            if (e.getErrorCode() == 1146 && "42s02".equals(e.getSQLState())) {
                LOG.warn("The shard info sql failed, the table will be treated as non-shard table, error message: ERROR 1146 (42s02) " + e.getMessage());
                return 0;
            } else {
                throw e;
            }
        }
    }

    public static boolean isShard(GenericOptions options, RDBMSManager manager) throws SQLException {
        return getShardNum(options, manager) != 0;
    }
}
