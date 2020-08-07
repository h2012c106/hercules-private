package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.StingyMap;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
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
import java.util.Map;

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

    protected RDBMSManager manager;

    protected Map<String, DataType> columnTypeMap;

    public RDBMSRecordReader(TaskAttemptContext context, RDBMSManager manager) {
        super(context);
        this.manager = manager;
    }

    protected String makeSql(GenericOptions sourceOptions, InputSplit split) {
        String querySql = SqlUtils.makeBaseQuery(sourceOptions);
        String splitBoundary = String.format("%s AND %s", ((RDBMSInputSplit) split).getLowerClause(),
                ((RDBMSInputSplit) split).getUpperClause());
        return SqlUtils.addWhere(querySql, splitBoundary);
    }

    protected void start(String querySql, Integer fetchSize) throws IOException {
        try {
            connection = manager.getConnection();
            statement = connection.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            SqlUtils.setFetchSize(statement, fetchSize);
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

        columnTypeMap = new StingyMap<>(getSchema().getColumnTypeMap());

        mapAverageRowNum = configuration.getLong(RDBMSInputFormat.AVERAGE_MAP_ROW_NUM, 0L);

        String querySql = makeSql(options.getSourceOptions(), split);

        Integer fetchSize = options.getSourceOptions().getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);

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

            int columnNum = getSchema().getColumnNameList().size();
            value = new HerculesWritable(columnNum);
            for (int i = 0; i < columnNum; ++i) {
                String columnName = getSchema().getColumnNameList().get(i);
                value.put(columnName, getWrapperGetter(columnTypeMap.get(columnName)).get(resultSet, null, null, i + 1));
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
