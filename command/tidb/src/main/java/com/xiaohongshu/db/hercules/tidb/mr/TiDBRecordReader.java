package com.xiaohongshu.db.hercules.tidb.mr;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSBalanceSplitGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputSplit;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSRecordReader;
import com.xiaohongshu.db.hercules.rdbms.mr.input.SplitUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.tidb.option.TiDBInputOptionsConf.SECONDARY_SPLIT_SIZE;

public class TiDBRecordReader extends RDBMSRecordReader {

    private static final Log LOG = LogFactory.getLog(TiDBRecordReader.class);

    private Long totalSize;
    private Iterator<String> querySqlIterator;

    @GeneralAssembly
    private RDBMSManager manager;

    @GeneralAssembly
    private RDBMSSchemaFetcher schemaFetcher;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

    private long splitSize;

    public TiDBRecordReader(TaskAttemptContext context) {
        super(context);
    }

    private long getSplitSize(String querySql) throws IOException {
        String countSql = SqlUtils.replaceSelectItem(querySql, SqlUtils.makeItem("COUNT", 1));
        try {
            return manager.executeSelect(countSql, 1, ResultSetGetter.LONG_GETTER).get(0);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void start(String querySql, Integer fetchSize) throws IOException {
        // 查一下本split行数，根据二层split大小计算需要多少split
        totalSize = getSplitSize(querySql);
        LOG.info("Split total size: " + totalSize);
        long splitSize = sourceOptions.getLong(SECONDARY_SPLIT_SIZE, null);
        int secondaryNumSplits = OverflowUtils.numberToInteger(Math.ceil((double) totalSize / (double) splitSize));
        SplitUtils.SplitResult secondarySplitResult = SplitUtils.split(sourceOptions, schema, schemaFetcher, secondaryNumSplits,
                manager, querySql, new RDBMSBalanceSplitGetter());
        querySqlIterator = secondarySplitResult.getInputSplitList()
                .stream()
                .map(split -> makeSql(sourceOptions, (RDBMSInputSplit) split))
                .collect(Collectors.toList()).iterator();

        statement = null;
        resultSet = null;
        try {
            connection = manager.getConnection();
        } catch (SQLException e) {
            close();
            throw new IOException(e);
        }
    }

    private boolean executeNewSplit() throws SQLException {
        splitSize = 0L;
        if (querySqlIterator.hasNext()) {
            String querySql = querySqlIterator.next();
            statement = SqlUtils.makeReadStatement(connection, querySql);
            LOG.info("Executing query: " + querySql);
            resultSet = statement.executeQuery();
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected boolean hasNext() throws SQLException {
        if (resultSet == null) {
            if (!executeNewSplit()) {
                return false;
            }
        }
        ++splitSize;
        while (!resultSet.next()) {
            LOG.info("Split size: " + (splitSize - 1L));
            // 先清理一波生命周期
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.warn("SQLException closing resultSet: " + ExceptionUtils.getStackTrace(e));
            }
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn("SQLException closing statement: " + ExceptionUtils.getStackTrace(e));
            }

            if (!executeNewSplit()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (totalSize == 0L) {
            return 0.0f;
        } else {
            return Math.min(1.0f, pos.floatValue() / totalSize.floatValue());
        }
    }
}
