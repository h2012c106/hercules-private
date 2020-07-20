package com.xiaohongshu.db.hercules.myhub.mr.input;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlInputFormat;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.QUERY;
import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf.TABLE;

public class MyhubInputFormat extends MysqlInputFormat {

    private static final Log LOG = LogFactory.getLog(MyhubInputFormat.class);

    private static final String IS_SHARD_PROPERTY_NAME = "hercules.myhub.table.shard";

    /**
     * @return 0代表非shard表，否则为shard表shard数
     */
    private int getShardNum() throws SQLException {
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

    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        int shardNum;
        try {
            shardNum = getShardNum();
        } catch (SQLException e) {
            throw new IOException(e);
        }
        boolean isShard = shardNum != 0;
        // 记录到Configuration去，在createReader的时候再用
        context.getConfiguration().setBoolean(IS_SHARD_PROPERTY_NAME, isShard);

        if (!isShard) {
            LOG.info("The table is a non-shard table, split as normal mysql table.");
            return super.innerGetSplits(context, numSplits);
        } else {
            LOG.info("The table is a non-shard table, split by shard.");
            List<InputSplit> res;
            if (shardNum < numSplits) {
                // 如果shard数量不到split数量，则不再切了，虽然可以再根据where条件切，但是I.懒;II.muhub本身分表就是为了把大表拆小，分表不会再出现巨大的数据量，再细拆有点伪命题的意思。
                LOG.warn(String.format("Cannot split more than the myhub shard num, split vs shard num: %d vd %d",
                        numSplits, shardNum));
                res = new LinkedList<>();
                for (int i = 0; i < shardNum; ++i) {
                    res.add(new MyhubInputSplit(i));
                }
            } else {
                res = new ArrayList<>(numSplits);
                // 发牌
                for (int i = 0; i < numSplits; ++i) {
                    res.add(new MyhubInputSplit());
                }
                int i = 0;
                for (int seq = 0; seq < shardNum; ++seq) {
                    ((MyhubInputSplit) res.get(i)).add(seq);
                    i = ++i < numSplits ? i : 0;
                }
            }
            return res;
        }
    }

    @Override
    public HerculesRecordReader<ResultSet> innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        if (configuration.get(IS_SHARD_PROPERTY_NAME) == null) {
            throw new RuntimeException("The configuration property must be set, now missed: " + IS_SHARD_PROPERTY_NAME);
        }
        boolean isShard = configuration.getBoolean(IS_SHARD_PROPERTY_NAME, false);
        if (!isShard) {
            return super.innerCreateRecordReader(split, context);
        } else {
            return new MyhubRecordReader(context, manager);
        }
    }
}
