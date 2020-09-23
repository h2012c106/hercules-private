package com.xiaohongshu.db.hercules.myhub.mr.input;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.myhub.MyhubUtils;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
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

public class MyhubInputFormat extends RDBMSInputFormat {

    private static final Log LOG = LogFactory.getLog(MyhubInputFormat.class);

    private static final String IS_SHARD_PROPERTY_NAME = "hercules.myhub.table.shard";

    @Assembly
    private RDBMSManager manager;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        int shardNum;
        try {
            shardNum = MyhubUtils.getShardNum(options, manager);
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
            LOG.info("The table is a shard table, split by shard.");
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

            String countSql = "/*MYHUB SHARD_NODES:0; SLAVE_PREFER */" + SqlUtils.replaceSelectItem(baseSql, SqlUtils.makeItem("COUNT", 1));
            LOG.info("Use sql to count the first shard size: " + countSql);
            long size;
            try {
                size = manager.executeSelect(countSql, 1, ResultSetGetter.LONG_GETTER).get(0);
            } catch (SQLException e) {
                throw new IOException(e);
            }
            LOG.info("Count size for shard 0 is: " + size);
            // 只是借这个名字一用，实际存储的值为每个shard的size而非每个map的size
            context.getConfiguration().setLong(AVERAGE_MAP_ROW_NUM, size);

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
            return new MyhubRecordReader(context);
        } else {
            return new MyhubShardRecordReader(context);
        }
    }
}
