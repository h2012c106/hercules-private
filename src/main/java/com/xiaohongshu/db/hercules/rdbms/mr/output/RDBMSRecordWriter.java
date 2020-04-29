package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.MultiThreadAsyncWriter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.StingyMap;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetterFactory;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class RDBMSRecordWriter extends HerculesRecordWriter<PreparedStatement> {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordWriter.class);

    private Long recordPerStatement;

    protected StatementGetter statementGetter;

    protected String tableName;

    private final boolean autocommit;
    private final TransactionManager transactionManager;
    private final Long statementPerCommit;
    private final boolean unlimitedStatementPerCommit;
    private final RDBMSManager manager;

    private final RDBMSMultiThreadAsyncWriter writer;

    /**
     * 键为列mask，上游有可能送来残缺的信息（各种缺列），对不同方式缺列的record做归并
     */
    private Map<String, List<HerculesWritable>> recordListMap;
    private Map<ColumnRowKey, String> sqlCache;

    abstract protected PreparedStatement getPreparedStatement(RDBMSWorkerMission mission, Connection connection)
            throws Exception;

    public RDBMSRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType, RDBMSManager manager)
            throws Exception {
        super(context);

        this.manager = manager;

        columnTypeMap = new StingyMap<>(super.columnTypeMap);

        this.tableName = tableName;
        statementGetter = StatementGetterFactory.get(exportType);

        recordPerStatement = options.getTargetOptions().getLong(RDBMSOutputOptionsConf.RECORD_PER_STATEMENT,
                RDBMSOutputOptionsConf.DEFAULT_RECORD_PER_STATEMENT);

        recordListMap = new HashMap<>();
        sqlCache = new HashMap<>();

        autocommit = options.getTargetOptions().getBoolean(RDBMSOutputOptionsConf.AUTOCOMMIT, false);
        transactionManager = autocommit ? TransactionManager.NULL : TransactionManager.NORMAL;
        statementPerCommit = options.getTargetOptions().getLong(RDBMSOutputOptionsConf.STATEMENT_PER_COMMIT, null);
        unlimitedStatementPerCommit = statementPerCommit == null;

        int threadNum = options.getTargetOptions().getInteger(RDBMSOutputOptionsConf.EXECUTE_THREAD_NUM,
                RDBMSOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM);
        writer = new RDBMSMultiThreadAsyncWriter(threadNum);
        writer.run();
    }

    abstract protected String makeSql(String columnMask, Integer rowNum);

    private String getSql(String columnMask, Integer rowNum) {
        ColumnRowKey key = new ColumnRowKey(columnMask, rowNum);
        return sqlCache.computeIfAbsent(key, k -> makeSql(k.getColumnMask(), k.getRowNum()));
    }

    abstract protected boolean singleRowPerSql();

    private void execUpdate(String columnMask, List<HerculesWritable> recordList) throws IOException, InterruptedException {
        if (recordList.size() <= 0) {
            return;
        }

        List<HerculesWritable> copiedRecordList = new ArrayList<HerculesWritable>(recordList);
        recordList.clear();

        // 如果是一sql只插一条数据，那么row num永远为1，与record size无关
        String sql = getSql(columnMask, singleRowPerSql() ? 1 : copiedRecordList.size());
        // 阻塞塞任务
        writer.put(new RDBMSWorkerMission(copiedRecordList, false, sql));
    }

    /**
     * 返回目标数据源需要的列是否在{@link HerculesWritable}内，每一位由0/1表示，转成10进制压缩
     *
     * @param value
     * @return
     */
    private String getWritableColumnMask(HerculesWritable value) {
        StringBuilder sb = new StringBuilder();
        for (String columnName : columnNameList) {
            sb.append(value.getRow().containsColumn(columnName) ? "1" : "0");
        }
        return sb.toString();
    }

    @Override
    public void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException {
        String columnMask = getWritableColumnMask(value);
        List<HerculesWritable> tmpRecordList = recordListMap.computeIfAbsent(columnMask,
                k -> new ArrayList<>(recordPerStatement.intValue()));
        tmpRecordList.add(value);
        if (tmpRecordList.size() >= recordPerStatement) {
            execUpdate(columnMask, tmpRecordList);
        }
    }

    @Override
    protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        // 把没凑满的缓存内容全部flush掉
        for (Map.Entry<String, List<HerculesWritable>> entry : recordListMap.entrySet()) {
            String columnMask = entry.getKey();
            List<HerculesWritable> tmpRecordList = entry.getValue();
            if (tmpRecordList.size() > 0) {
                execUpdate(columnMask, tmpRecordList);
            }
        }

        writer.done();
    }

    @Override
    protected WrapperSetter<PreparedStatement> getIntegerSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String name, int seq) throws Exception {
                Long res = wrapper.asLong();
                if (res == null) {
                    row.setNull(seq, Types.BIGINT);
                } else {
                    row.setLong(seq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDoubleSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String name, int seq) throws Exception {
                BigDecimal res = wrapper.asBigDecimal();
                if (res == null) {
                    row.setNull(seq, Types.NUMERIC);
                } else {
                    row.setBigDecimal(seq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getBooleanSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String name, int seq) throws Exception {
                Boolean res = wrapper.asBoolean();
                if (res == null) {
                    row.setNull(seq, Types.BOOLEAN);
                } else {
                    row.setBoolean(seq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getStringSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String name, int seq) throws Exception {
                String res = wrapper.asString();
                if (res == null) {
                    row.setNull(seq, Types.VARCHAR);
                } else {
                    row.setString(seq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDateSetter() {
        return getStringSetter();
    }

    @Override
    protected WrapperSetter<PreparedStatement> getBytesSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String name, int seq) throws Exception {
                byte[] res = wrapper.asBytes();
                if (res == null) {
                    row.setNull(seq, Types.LONGVARBINARY);
                } else {
                    row.setBytes(seq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getNullSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String name, int seq) throws Exception {
                row.setNull(seq, Types.NULL);
            }
        };
    }

    private class RDBMSMultiThreadAsyncWriter extends MultiThreadAsyncWriter<RDBMSMultiThreadAsyncWriter.ThreadContext, RDBMSWorkerMission> {

        public RDBMSMultiThreadAsyncWriter(int threadNum) {
            super(threadNum);
        }

        @Override
        protected ThreadContext initializeThreadContext() throws Exception {
            Connection connection = manager.getConnection();
            connection.setAutoCommit(autocommit);
            return new ThreadContext(connection);
        }

        @Override
        protected void doWrite(ThreadContext context, RDBMSWorkerMission mission) throws Exception {
            if (mission.getHerculesWritableList() != null && mission.getHerculesWritableList().size() > 0) {
                context.addRecordNum(mission.getHerculesWritableList().size());
                PreparedStatement preparedStatement = null;
                try {
                    preparedStatement = getPreparedStatement(mission, context.getConnection());
                    mission.clearHerculesWritableList();
                    preparedStatement.executeBatch();
                    context.increaseTmpStatementPerCommit();
                    context.increaseExecuteNum();
                } finally {
                    if (preparedStatement != null) {
                        try {
                            preparedStatement.close();
                        } catch (SQLException ignore) {
                        }
                    }
                }
            }

            if (mission.needClose()
                    || (!unlimitedStatementPerCommit && context.getTmpStatementPerCommit() >= statementPerCommit)) {
                transactionManager.commit(context.getConnection());
                context.setTmpStatementPerCommit(0);
                context.increaseCommitNum();
            }
        }

        @Override
        protected void handleException(ThreadContext context, RDBMSWorkerMission mission, Exception e) {
            context.increaseErrorNum();
        }

        @Override
        protected void closeContext(ThreadContext context) {
            if (context.getConnection() != null) {
                try {
                    context.getConnection().close();
                } catch (SQLException ignore) {
                }
            }
            LOG.info(String.format("Thread %s done with %d errors, execute %d records in %d executes / %d commits",
                    Thread.currentThread().getName(),
                    context.getErrorNum(),
                    context.getRecordNum(),
                    context.getExecuteNum(),
                    context.getCommitNum()));
        }

        @Override
        protected RDBMSWorkerMission innerGetCloseMission() {
            return new RDBMSWorkerMission(null, true, null);
        }

        private class ThreadContext {
            private Connection connection;
            private long tmpStatementPerCommit = 0L;
            private long recordNum = 0L;
            private long executeNum = 0L;
            private long commitNum = 0L;
            private long errorNum = 0L;

            public ThreadContext(Connection connection) {
                this.connection = connection;
            }

            public Connection getConnection() {
                return connection;
            }

            public long getTmpStatementPerCommit() {
                return tmpStatementPerCommit;
            }

            public long getRecordNum() {
                return recordNum;
            }

            public long getExecuteNum() {
                return executeNum;
            }

            public long getCommitNum() {
                return commitNum;
            }

            public long getErrorNum() {
                return errorNum;
            }

            public void setTmpStatementPerCommit(long tmpStatementPerCommit) {
                this.tmpStatementPerCommit = tmpStatementPerCommit;
            }

            public void increaseTmpStatementPerCommit() {
                ++tmpStatementPerCommit;
            }

            public void addRecordNum(long recordNum) {
                this.recordNum += recordNum;
            }

            public void increaseExecuteNum() {
                ++executeNum;
            }

            public void increaseCommitNum() {
                ++commitNum;
            }

            public void increaseErrorNum() {
                ++errorNum;
            }
        }
    }

    public static class RDBMSWorkerMission extends MultiThreadAsyncWriter.WorkerMission {
        private List<HerculesWritable> herculesWritableList;
        private String sql;

        public RDBMSWorkerMission(List<HerculesWritable> herculesWritableList, boolean close, String sql) {
            super(close);
            this.herculesWritableList = herculesWritableList;
            this.sql = sql;
        }

        public void clearHerculesWritableList() {
            herculesWritableList.clear();
        }

        public List<HerculesWritable> getHerculesWritableList() {
            return herculesWritableList;
        }

        public void setHerculesWritableList(List<HerculesWritable> herculesWritableList) {
            this.herculesWritableList = herculesWritableList;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }
    }

    abstract private static class TransactionManager {
        public static final TransactionManager NORMAL = new TransactionManager() {
            @Override
            void commit(Connection connection) throws SQLException {
                connection.commit();
            }

            @Override
            void rollback(Connection connection) throws SQLException {
                connection.rollback();
            }
        };
        public static final TransactionManager NULL = new TransactionManager() {
            @Override
            void commit(Connection connection) throws SQLException {
            }

            @Override
            void rollback(Connection connection) throws SQLException {
            }
        };

        abstract void commit(Connection connection) throws SQLException;

        abstract void rollback(Connection connection) throws SQLException;
    }

    /**
     * 用于唯一标示n列(排列组合)m行的一条sql
     */
    protected static class ColumnRowKey {
        private String columnMask;
        private Integer rowNum;

        public ColumnRowKey(String columnMask, Integer rowNum) {
            this.columnMask = columnMask;
            this.rowNum = rowNum;
        }

        public String getColumnMask() {
            return columnMask;
        }

        public Integer getRowNum() {
            return rowNum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnRowKey that = (ColumnRowKey) o;
            return Objects.equal(columnMask, that.columnMask) &&
                    Objects.equal(rowNum, that.rowNum);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(columnMask, rowNum);
        }
    }
}
