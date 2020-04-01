package com.xiaohongshu.db.hercules.rdbms.input.mr;

import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.core.serialize.datatype.*;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RDBMSRecordReader extends RecordReader<NullWritable, HerculesWritable> {

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
    /**
     * 事先记录好每个下标对应的列如何转换到base wrapper的方法，不用每次读到一列就switch...case了
     */
    private List<WrapperGetter> wrapperGetterList;

    private AtomicBoolean hasClosed = new AtomicBoolean(false);

    private List<WrapperGetter> makeWrapperGetterList(RDBMSSchemaFetcher schemaFetcher) {
        return schemaFetcher.getColumnNameList()
                .stream()
                .map(columnName
                        -> WrapperGetter.FACTORY.get(
                        schemaFetcher.getColumnTypeMap().get(columnName)
                ))
                .collect(Collectors.toList());
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        mapAverageRowNum = configuration.getLong(RDBMSInputFormat.AVERAGE_MAP_ROW_NUM, 0L);

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        RDBMSSchemaFetcher schemaFetcher = SchemaFetcherFactory.getSchemaFetcher(options.getSourceOptions(),
                RDBMSSchemaFetcher.class);
        RDBMSManager manager = schemaFetcher.getManager();

        String querySql = schemaFetcher.getQuerySql();
        RDBMSInputSplit rdbmsInputSplit = (RDBMSInputSplit) split;
        String splitBoundary = String.format("%s AND %s", rdbmsInputSplit.getLowerClause(),
                rdbmsInputSplit.getUpperClause());
        querySql = SqlUtils.addWhere(querySql, splitBoundary);

        wrapperGetterList = makeWrapperGetterList(schemaFetcher);

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
            throw new IOException(e);
        } finally {
            close();
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

            int columnNum = wrapperGetterList.size();
            value = new HerculesWritable(columnNum);
            for (int i = 0; i < columnNum; ++i) {
                value.append(wrapperGetterList.get(i).get(resultSet, i + 1));
            }

            return true;
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            close();
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

    abstract public static class WrapperGetter {
        public static final WrapperGetter INTEGER_GETTER = new WrapperGetter() {
            @Override
            public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException {
                Long res = resultSet.getLong(seq);
                if (resultSet.wasNull()) {
                    return new NullWrapper();
                } else {
                    return new IntegerWrapper(res);
                }
            }
        };
        public static final WrapperGetter DOUBLE_GETTER = new WrapperGetter() {
            @Override
            public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException {
                BigDecimal res = resultSet.getBigDecimal(seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new DoubleWrapper(res);
                }
            }
        };
        public static final WrapperGetter BOOLEAN_GETTER = new WrapperGetter() {
            @Override
            public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException {
                Boolean res = resultSet.getBoolean(seq);
                if (resultSet.wasNull()) {
                    return new NullWrapper();
                } else {
                    return new BooleanWrapper(res);
                }
            }
        };
        public static final WrapperGetter STRING_GETTER = new WrapperGetter() {
            @Override
            public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException {
                String res = resultSet.getString(seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new StringWrapper(res);
                }
            }
        };
        public static final WrapperGetter DATE_GETTER = new WrapperGetter() {
            @Override
            public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException {
                String res = SqlUtils.getTimestamp(resultSet, seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new DateWrapper(res);
                }
            }
        };
        public static final WrapperGetter BYTES_GETTER = new WrapperGetter() {
            @Override
            public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException {
                byte[] res = resultSet.getBytes(seq);
                if (res == null) {
                    return new NullWrapper();
                } else {
                    return new BytesWrapper(res);
                }
            }
        };
        public static final WrapperGetter NULL_GETTER = new WrapperGetter() {
            @Override
            public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException {
                return new NullWrapper();
            }
        };
        public static final Map<DataType, WrapperGetter> FACTORY = new HashMap<>();

        static {
            FACTORY.put(DataType.INTEGER, INTEGER_GETTER);
            FACTORY.put(DataType.DOUBLE, DOUBLE_GETTER);
            FACTORY.put(DataType.BOOLEAN, BOOLEAN_GETTER);
            FACTORY.put(DataType.STRING, STRING_GETTER);
            FACTORY.put(DataType.DATE, DATE_GETTER);
            FACTORY.put(DataType.BYTES, BYTES_GETTER);
            FACTORY.put(DataType.NULL, NULL_GETTER);
        }


        abstract public BaseWrapper get(ResultSet resultSet, int seq) throws SQLException;
    }
}
