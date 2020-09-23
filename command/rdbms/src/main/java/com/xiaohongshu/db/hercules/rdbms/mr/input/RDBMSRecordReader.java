package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.core.utils.entity.StingyMap;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RDBMSRecordReader extends HerculesRecordReader<ResultSet> {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordReader.class);

    protected Long pos = 0L;
    /**
     * 用于估算进度
     */
    protected Long mapAverageRowNum;
    protected Connection connection = null;
    protected PreparedStatement statement = null;
    protected ResultSet resultSet = null;
    protected HerculesWritable value;

    @Assembly(role = DataSourceRole.SOURCE)
    private RDBMSManager manager;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    public RDBMSRecordReader(TaskAttemptContext context) {
        super(context);
    }

    protected String makeSplitSql(String querySql, InputSplit split) {
        String splitBoundary = String.format("%s AND %s", ((RDBMSInputSplit) split).getLowerClause(),
                ((RDBMSInputSplit) split).getUpperClause());
        querySql = SqlUtils.addWhere(querySql, splitBoundary);
        return querySql;
    }

    protected String makeSql(String querySql, InputSplit split) {
        return makeSql(querySql, split, true);
    }

    /**
     * @param querySql
     * @param split
     * @param considerFilter tidb二次切分后再拼sql时，若再无脑用这个函数会导致filter参数又重新加一遍，没必要
     * @return
     */
    protected String makeSql(String querySql, InputSplit split, boolean considerFilter) {
        querySql = makeSplitSql(querySql, split);
        if (considerFilter) {
            String filterQuery = (String) getFilter();
            if (filterQuery != null) {
                querySql = SqlUtils.addWhere(querySql, filterQuery);
            }
        }
        return querySql;
    }

    protected void start(String querySql, Integer fetchSize) throws IOException {
        try {
            connection = manager.getConnection();
            statement = SqlUtils.makeReadStatement(connection, querySql, fetchSize);
            LOG.info("Executing query: " + querySql);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        schema.setColumnTypeMap(new StingyMap<>(schema.getColumnTypeMap()));

        mapAverageRowNum = configuration.getLong(RDBMSInputFormat.AVERAGE_MAP_ROW_NUM, 0L);

        String querySql = makeSql(manager.makeBaseQuery(), split);

        Integer fetchSize = sourceOptions.getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);

        start(querySql, fetchSize);
    }

    protected boolean hasNext() throws SQLException {
        return resultSet.next();
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        try {
            if (!hasNext()) {
                return false;
            }

            ++pos;

            int columnNum = schema.getColumnNameList().size();
            value = new HerculesWritable(columnNum);
            for (int i = 0; i < columnNum; ++i) {
                String columnName = schema.getColumnNameList().get(i);
                value.put(columnName, getWrapperGetter(schema.getColumnTypeMap().get(columnName)).get(resultSet, null, null, i + 1));
            }

            return true;
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    protected boolean isDone() {
        try {
            return resultSet != null && (resultSet.isClosed() || resultSet.isAfterLast());
        } catch (SQLException sqlE) {
            return true;
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (isDone()) {
            return 1.0f;
        } else {
            if (mapAverageRowNum == 0L) {
                return 0.0f;
            } else {
                return Math.min(1.0f, pos.floatValue() / mapAverageRowNum.floatValue());
            }
        }
    }

    @Override
    public void innerClose() throws IOException {
        LOG.info(String.format("Selected %d records.", pos));
        SqlUtils.release(Lists.newArrayList(resultSet, statement, connection));
    }
}
