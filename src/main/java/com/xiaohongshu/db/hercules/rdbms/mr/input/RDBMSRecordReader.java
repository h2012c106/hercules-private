package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.*;
import com.xiaohongshu.db.hercules.core.utils.StingyMap;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class RDBMSRecordReader extends HerculesRecordReader<ResultSet, RDBMSDataTypeConverter> {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordReader.class);

    private Long pos = 0L;
    /**
     * 用于估算进度
     */
    private Long mapAverageRowNum;
    private Connection connection = null;
    private PreparedStatement statement = null;
    private ResultSet resultSet = null;
    private HerculesWritable value;

    private RDBMSManager manager;

    private AtomicBoolean hasClosed = new AtomicBoolean(false);

    public RDBMSRecordReader(RDBMSManager manager, RDBMSDataTypeConverter converter) {
        super(converter);
        this.manager = manager;
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        columnTypeMap = new StingyMap<>(super.columnTypeMap);

        mapAverageRowNum = configuration.getLong(RDBMSInputFormat.AVERAGE_MAP_ROW_NUM, 0L);

        String querySql = SqlUtils.makeBaseQuery(options.getSourceOptions());
        RDBMSInputSplit rdbmsInputSplit = (RDBMSInputSplit) split;
        String splitBoundary = String.format("%s AND %s", rdbmsInputSplit.getLowerClause(),
                rdbmsInputSplit.getUpperClause());
        querySql = SqlUtils.addWhere(querySql, splitBoundary);

        Integer fetchSize = options.getSourceOptions().getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);

        try {
            connection = manager.getConnection();
            statement = connection.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (fetchSize != null) {
                LOG.info("Using fetchSize for query: " + fetchSize);
                statement.setFetchSize(fetchSize);
            }
            LOG.info("Executing query: " + querySql);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (!resultSet.next()) {
                LOG.info(String.format("Selected %d records.", pos));
                return false;
            }

            ++pos;

            int columnNum = columnNameList.size();
            value = new HerculesWritable(columnNum);
            for (int i = 0; i < columnNum; ++i) {
                String columnName = columnNameList.get(i);
                value.put(columnName, getWrapperGetter(columnTypeMap.get(columnName)).get(resultSet, null, i + 1));
            }

            return true;
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public HerculesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    private boolean isDone() {
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
    public void close() throws IOException {
        if (!hasClosed.getAndSet(true)) {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing resultSet: " + ExceptionUtils.getStackTrace(e));
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing statement: " + ExceptionUtils.getStackTrace(e));
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing connection: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    @Override
    protected WrapperGetter<ResultSet> getIntegerGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                Long res = resultSet.getLong(seq);
                if (resultSet.wasNull()) {
                    return new NullWrapper();
                } else {
                    return new IntegerWrapper(res);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getDoubleGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                BigDecimal res = resultSet.getBigDecimal(seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new DoubleWrapper(res);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getBooleanGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                Boolean res = resultSet.getBoolean(seq);
                if (resultSet.wasNull()) {
                    return new NullWrapper();
                } else {
                    return new BooleanWrapper(res);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getStringGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                String res = resultSet.getString(seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new StringWrapper(res);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getDateGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                String res = SqlUtils.getTimestamp(resultSet, seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new DateWrapper(res);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getBytesGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                byte[] res = resultSet.getBytes(seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new BytesWrapper(res);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getNullGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                return NullWrapper.INSTANCE;
            }
        };
    }
}
