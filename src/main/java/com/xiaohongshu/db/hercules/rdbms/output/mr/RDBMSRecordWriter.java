package com.xiaohongshu.db.hercules.rdbms.output.mr;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;
import com.xiaohongshu.db.hercules.rdbms.output.mr.statement.StatementGetter;
import com.xiaohongshu.db.hercules.rdbms.output.mr.statement.StatementGetterFactory;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;
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
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

abstract public class RDBMSRecordWriter extends HerculesRecordWriter<PreparedStatement, RDBMSSchemaFetcher> {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordWriter.class);

    private boolean closed = false;
    private Long recordPerStatement;

    protected StatementGetter statementGetter;

    protected String tableName;

    private int threadNum;
    private ExecutorService threadPool;
    final private BlockingQueue<WorkerMission> missionQueue = new SynchronousQueue<WorkerMission>();
    final private List<Exception> exceptionList = new ArrayList<Exception>();
    private AtomicBoolean threadPoolClosed = new AtomicBoolean(false);

    private List<HerculesWritable> recordList;

    abstract protected PreparedStatement getPreparedStatement(List<HerculesWritable> recordList, Connection connection)
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
                                    preparedStatement = getPreparedStatement(mission.getHerculesWritableList(), connection);
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

        recordList = new ArrayList<>(recordPerStatement.intValue());

        generateThreadPool(schemaFetcher);
    }

    private void closeThreadPool() throws InterruptedException {
        if (!threadPoolClosed.getAndSet(true)) {
            // 起了多少个线程就发多少个停止命令，在worker逻辑中已经保证了错误不会导致不再take，且threadPoolClosed保证此逻辑只会走一次
            for (int i = 0; i < threadNum; ++i) {
                missionQueue.put(new WorkerMission(null, true));
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

    private void execUpdate() throws IOException, InterruptedException {
        // 先检查有没有抛错
        checkException();

        if (recordList.size() <= 0) {
            return;
        }

        List<HerculesWritable> copiedRecordList = new ArrayList<HerculesWritable>(recordList);
        recordList.clear();
        // 阻塞塞任务
        missionQueue.put(new WorkerMission(copiedRecordList, false));
    }

    @Override
    public void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        recordList.add(value);
        if (recordList.size() >= recordPerStatement) {
            execUpdate();
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (closed) {
            return;
        }
        closed = true;

        checkException();

        if (recordList.size() > 0) {
            execUpdate();
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

        public WorkerMission(List<HerculesWritable> herculesWritableList, boolean close) {
            this.herculesWritableList = herculesWritableList;
            this.close = close;
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
    }

    abstract public static class TransactionManager {
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
}
