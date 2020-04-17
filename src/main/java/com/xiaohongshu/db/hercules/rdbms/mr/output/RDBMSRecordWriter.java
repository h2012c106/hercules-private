package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetterFactory;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

abstract public class RDBMSRecordWriter extends HerculesRecordWriter<PreparedStatement, RDBMSSchemaFetcher> {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordWriter.class);

    private static final boolean COLUMN_NAME_ONE_LEVEL = true;

    private boolean closed = false;
    private Long recordPerStatement;

    protected StatementGetter statementGetter;

    protected String tableName;

    private int threadNum;
    private ExecutorService threadPool;
    final private BlockingQueue<WorkerMission> missionQueue = new SynchronousQueue<WorkerMission>();
    final private List<Exception> exceptionList = new ArrayList<Exception>();
    private AtomicBoolean threadPoolClosed = new AtomicBoolean(false);

    /**
     * 键为列mask，上游有可能送来残缺的信息（各种缺列），对不同方式缺列的record做归并
     */
    private Map<Integer, List<HerculesWritable>> recordListMap;
    private Map<ColumnRowKey, String> sqlCache;

    abstract protected PreparedStatement getPreparedStatement(WorkerMission mission, Connection connection)
            throws Exception;

    private void generateThreadPool(final RDBMSSchemaFetcher schemaFetcher) throws SQLException, ClassNotFoundException {
        final GenericOptions options = schemaFetcher.getOptions();
        final RDBMSManager manager = schemaFetcher.getManager();

        threadNum = options.getInteger(RDBMSOutputOptionsConf.EXECUTE_THREAD_NUM,
                RDBMSOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM);

        final boolean autocommit = options.getBoolean(RDBMSOutputOptionsConf.AUTOCOMMIT, false);
        final TransactionManager transactionManager = autocommit ? TransactionManager.NULL : TransactionManager.NORMAL;
        final Long statementPerCommit = options.getLong(RDBMSOutputOptionsConf.STATEMENT_PER_COMMIT, null);
        final boolean unlimitedStatementPerCommit = statementPerCommit == null;

        threadPool = new ThreadPoolExecutor(threadNum,
                threadNum,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("Hercules-Export-Worker-%d").build(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        for (int i = 0; i < threadNum; ++i) {
            final Connection connection = manager.getConnection();
            connection.setAutoCommit(autocommit);
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info(String.format("Thread %s start, connection: %s",
                            Thread.currentThread().getName(),
                            connection.toString()));
                    long tmpStatementPerCommit = 0L;
                    long recordNum = 0L;
                    long executeNum = 0L;
                    long commitNum = 0L;
                    long errorNum = 0L;
                    while (true) {
                        // 从任务队列里阻塞取
                        WorkerMission mission = null;
                        try {
                            mission = missionQueue.take();
                        } catch (InterruptedException e) {
                            LOG.warn("Worker's taking mission interrupted: " + ExceptionUtils.getStackTrace(e));
                            continue;
                        }

                        if (mission == null) {
                            LOG.warn("Null mission");
                            continue;
                        }

                        // try...catch原因在于不能因为一次循环里的错误就让循环断掉，必须得留着循环，其实是必须留着take方法
                        // 考虑这么一种情况，在close时最后一次execUpdate崩了，如果出循环了那么主线程停止命令将永远阻塞在put上，
                        // 或者这种情况，在一次execUpdate头部check未检查出错误，但是在主线程阻塞提交任务时，
                        // n个线程全崩了且跳出循环了，此时无人take，死锁
                        try {
                            if (mission.getHerculesWritableList() != null && mission.getHerculesWritableList().size() > 0) {
                                recordNum += mission.getHerculesWritableList().size();
                                PreparedStatement preparedStatement = null;
                                try {
                                    preparedStatement = getPreparedStatement(mission, connection);
                                    mission.clearHerculesWritableList();
                                    preparedStatement.executeBatch();
                                    ++tmpStatementPerCommit;
                                    ++executeNum;
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
                                    || (!unlimitedStatementPerCommit && tmpStatementPerCommit >= statementPerCommit)) {
                                transactionManager.commit(connection);
                                tmpStatementPerCommit = 0;
                                ++commitNum;
                            }
                        } catch (Exception e) {
                            ++errorNum;
                            exceptionList.add(e);
                        }

                        if (mission.needClose()) {
                            break;
                        }
                    }
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException ignore) {
                        }
                    }
                    LOG.info(String.format("Thread %s done with %d errors, execute %d records in %d executes / %d commits",
                            Thread.currentThread().getName(),
                            errorNum,
                            recordNum,
                            executeNum,
                            commitNum));
                }
            });
        }
    }

    public RDBMSRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType, RDBMSSchemaFetcher schemaFetcher)
            throws SQLException, ClassNotFoundException {
        super(context, schemaFetcher);

        this.tableName = tableName;
        statementGetter = StatementGetterFactory.get(exportType);

        recordPerStatement = options.getTargetOptions().getLong(RDBMSOutputOptionsConf.RECORD_PER_STATEMENT,
                RDBMSOutputOptionsConf.DEFAULT_RECORD_PER_STATEMENT);

        recordListMap = new HashMap<>();
        sqlCache = new HashMap<>();

        generateThreadPool(schemaFetcher);
    }

    @Override
    protected boolean isColumnNameOneLevel() {
        return COLUMN_NAME_ONE_LEVEL;
    }

    private void closeThreadPool() throws InterruptedException {
        if (!threadPoolClosed.getAndSet(true)) {
            // 起了多少个线程就发多少个停止命令，在worker逻辑中已经保证了错误不会导致不再take，且threadPoolClosed保证此逻辑只会走一次
            for (int i = 0; i < threadNum; ++i) {
                missionQueue.put(new WorkerMission(null, true, null));
            }
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    private void checkException() throws IOException, InterruptedException {
        if (exceptionList.size() > 0) {
            // just for elegance，直接抛也可以
            closeThreadPool();
            throw new IOException(exceptionList.get(0));
        }
    }

    abstract protected String makeSql(String columnMask, Integer rowNum);

    private String getSql(Integer columnMask, Integer rowNum) {
        ColumnRowKey key = new ColumnRowKey(columnMask, rowNum);
        return sqlCache.computeIfAbsent(key, k -> makeSql(uncompressColumnMask(k.getColumnMaskHex()), k.getRowNum()));
    }

    abstract protected boolean singleRowPerSql();

    private void execUpdate(Integer columnMask, List<HerculesWritable> recordList) throws IOException, InterruptedException {
        // 先检查有没有抛错
        checkException();

        if (recordList.size() <= 0) {
            return;
        }

        List<HerculesWritable> copiedRecordList = new ArrayList<HerculesWritable>(recordList);
        recordList.clear();

        // 如果是一sql只插一条数据，那么row num永远为1，与record size无关
        String sql = getSql(columnMask, singleRowPerSql() ? 1 : recordList.size());
        // 阻塞塞任务
        missionQueue.put(new WorkerMission(copiedRecordList, false, sql));
    }

    private Integer compressColumnMask(String mask) {
        return Integer.parseInt(mask, 2);
    }

    private String uncompressColumnMask(Integer maskInt) {
        // 注意补齐左边的0
        String res = Integer.toBinaryString(maskInt);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (columnNames.length - res.length()); ++i) {
            sb.append("0");
        }
        return sb.append(res).toString();
    }

    /**
     * 返回目标数据源需要的列是否在{@link HerculesWritable}内，每一位由0/1表示，转成10进制压缩
     *
     * @param value
     * @return
     */
    private Integer getWritableColumnMaskHex(HerculesWritable value) {
        StringBuilder sb = new StringBuilder();
        for (String columnName : columnNames) {
            sb.append(value.getRow().containsColumn(columnName, COLUMN_NAME_ONE_LEVEL) ? "1" : "0");
        }
        return compressColumnMask(sb.toString());
    }

    @Override
    public void innerWrite(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        Integer columnMask = getWritableColumnMaskHex(value);
        List<HerculesWritable> tmpRecordList = recordListMap.computeIfAbsent(columnMask,
                k -> new ArrayList<>(recordPerStatement.intValue()));
        tmpRecordList.add(value);
        if (tmpRecordList.size() >= recordPerStatement) {
            execUpdate(columnMask, tmpRecordList);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (closed) {
            return;
        }
        closed = true;

        checkException();

        // 把没凑满的缓存内容全部flush掉
        for (Map.Entry<Integer, List<HerculesWritable>> entry : recordListMap.entrySet()) {
            Integer columnMask = entry.getKey();
            List<HerculesWritable> tmpRecordList = entry.getValue();
            if (tmpRecordList.size() > 0) {
                execUpdate(columnMask, tmpRecordList);
            }
        }

        closeThreadPool();

        checkException();
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

    public static class WorkerMission {
        private List<HerculesWritable> herculesWritableList;
        private boolean close;
        private String sql;

        public WorkerMission(List<HerculesWritable> herculesWritableList, boolean close, String sql) {
            this.herculesWritableList = herculesWritableList;
            this.close = close;
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

        public boolean needClose() {
            return close;
        }

        public void setClose(boolean close) {
            this.close = close;
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
        private Integer columnMaskHex;
        private Integer rowNum;

        public ColumnRowKey(Integer columnMaskHex, Integer rowNum) {
            this.columnMaskHex = columnMaskHex;
            this.rowNum = rowNum;
        }

        public Integer getColumnMaskHex() {
            return columnMaskHex;
        }

        public Integer getRowNum() {
            return rowNum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnRowKey that = (ColumnRowKey) o;
            return Objects.equal(columnMaskHex, that.columnMaskHex) &&
                    Objects.equal(rowNum, that.rowNum);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(columnMaskHex, rowNum);
        }
    }
}
