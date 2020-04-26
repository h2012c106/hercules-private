package com.xiaohongshu.db.hercules.hbase.mr;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSRecordWriter;
import lombok.NonNull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class HBaseOutputFormat extends HerculesOutputFormat implements HBaseManagerInitializer {

    private Configuration conf;
    private GenericOptions targetOptions;
    private HBaseManager manager;


    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        targetOptions = options.getTargetOptions();
        manager = initializeManager(targetOptions);
        conf = manager.getConf();
        String tableName = targetOptions.getString(HBaseOutputOptionsConf.OUTPU_TABLE, null);
        return new HBaseRecordWriter(manager, tableName, context);
    }

    // setup conf according to targetOptions
    private void setConf(){
        // TODO 完成conf的设置
        conf.set(HBaseOutputOptionsConf.COLUMN_FAMILY,
                targetOptions.getString(HBaseOutputOptionsConf.COLUMN_FAMILY,null));

        conf.setInt(HBaseOutputOptionsConf.EXECUTE_THREAD_NUM,
                targetOptions.getInteger(HBaseOutputOptionsConf.EXECUTE_THREAD_NUM, HBaseOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM));

        conf.setInt(HBaseOutputOptionsConf.PUT_BATCH_SIZE,
                targetOptions.getInteger(HBaseOutputOptionsConf.PUT_BATCH_SIZE,HBaseOutputOptionsConf.DEFAULT_PUT_BATCH_SIZE));

        conf.set(HBaseOutputOptionsConf.OUTPU_TABLE, targetOptions.getString(HBaseOutputOptionsConf.OUTPU_TABLE,null));

        conf.set(HBaseOutputOptionsConf.ROW_KEY_COL, targetOptions.getString(HBaseOutputOptionsConf.ROW_KEY_COL,null));

    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }
}

class HBaseRecordWriter extends HerculesRecordWriter {

    private Connection conn;
    private String columnFamily;
    private String rowKeyCol;
    private String tableName;
    private List<HerculesWritable> recordList;
    private Long putBatchSize;
    private HBaseManager manager;
    private static final Log LOG = LogFactory.getLog(RDBMSRecordWriter.class);

    // ThreadPool
    private int threadNum;
    private ExecutorService threadPool;
    private AtomicBoolean threadPoolClosed = new AtomicBoolean(false);
    final private List<Exception> exceptionList = new ArrayList<Exception>();
    final private BlockingQueue<HBaseRecordWriter.WorkerMission> missionQueue = new SynchronousQueue<HBaseRecordWriter.WorkerMission>();


    public HBaseRecordWriter(HBaseManager manager, String tableName, TaskAttemptContext context) throws IOException {
        super(context);

        this.manager = manager;
        this.tableName = tableName;
        Configuration conf = manager.getConf();
        conn = manager.getConnection();
        columnFamily = conf.get(HBaseOutputOptionsConf.COLUMN_FAMILY);
        rowKeyCol = conf.get(HBaseOutputOptionsConf.ROW_KEY_COL);

        // 目前相关的变量统一从 Configuration conf 中拿取
        putBatchSize = conf.getLong(HBaseOutputOptionsConf.PUT_BATCH_SIZE, HBaseOutputOptionsConf.DEFAULT_PUT_BATCH_SIZE);
        recordList = new ArrayList<>(putBatchSize.intValue());

        generateThreadPool(conf);
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
        missionQueue.put(new HBaseRecordWriter.WorkerMission(copiedRecordList, false));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        manager.closeConnection();
    }

    public void generateThreadPool(Configuration conf) throws IOException {


        threadNum = conf.getInt(HBaseOutputOptionsConf.EXECUTE_THREAD_NUM,
                HBaseOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM);
        threadPool = new ThreadPoolExecutor(threadNum,
                threadNum,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("Hercules-Export-Worker-%d").build(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        for (int i = 0; i < threadNum; ++i) {

            // get one HTable per each thread
            final HTable t = (HTable) conn.getTable(TableName.valueOf(tableName));
            threadPool.execute(new Runnable() {

                @Override
                public void run() {

                    LOG.info(String.format("Thread %s start, connection: %s",
                            Thread.currentThread().getName(),
                            t.toString()));
                    long recordNum = 0L;
                    long executeNum = 0L;
                    long commitNum = 0L;
                    long errorNum = 0L;
                    while(true){
                        // 从任务队列里阻塞取
                        HBaseRecordWriter.WorkerMission mission = null;
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

                        try {
                            if (mission.getHerculesWritableList() != null && mission.getHerculesWritableList().size() > 0) {
                                recordNum += mission.getHerculesWritableList().size();
                                // generate puts and put it to the table
                                List<Put> putList = generatePutList(mission.getHerculesWritableList());
                                mission.clearHerculesWritableList();
                                t.put(putList);
                                ++executeNum;
                            }
                        } catch (Exception e) {
                            ++errorNum;
                            exceptionList.add(e);
                        }

                        if (mission.needClose()) {
                            break;
                        }
                    }
                    if (t != null) {
                        try {
                            t.close();
                        } catch (IOException ignore) {
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

    private void closeThreadPool() throws InterruptedException {
        if (!threadPoolClosed.getAndSet(true)) {
            // 起了多少个线程就发多少个停止命令，在worker逻辑中已经保证了错误不会导致不再take，且threadPoolClosed保证此逻辑只会走一次
            for (int i = 0; i < threadNum; ++i) {
                missionQueue.put(new HBaseRecordWriter.WorkerMission(null, true));
            }
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    private void checkException() throws IOException, InterruptedException {
        if (exceptionList.size() > 0) {
            // just for elegance，直接抛也可以(保证线程中的HTable可以调用close()函数)
            closeThreadPool();
            throw new IOException(exceptionList.get(0));
        }
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


    public List<Put> generatePutList(List<HerculesWritable> herculesWritableList) throws Exception {

        List<Put> putList = new ArrayList<>();
        for(HerculesWritable record: herculesWritableList){
            putList.add(generatePut(record));
        }
        return putList;
    }

    /**
     * @param record
     * 根据是否提供不为空的columnNameList来生成PUT
     */
    public Put generatePut(HerculesWritable record) throws Exception {

        // TODO 更新row_key, row key 来自上游，可能存储在value里面, 暂时不支持 compositeRowKeyCol
        Put put = new Put(rowKeyCol.getBytes());
        BaseWrapper wrapper;
        if(columnNameList.size()==0){
            for(Map.Entry colVal: record.getRow().entrySet()){
                String qualifier = (String) colVal.getKey();
                wrapper = (BaseWrapper) colVal.getValue();
                constructPut(put, wrapper, qualifier);
            }
        }else{
            // 如果存在 columnNameList， 则以 columnNameList 为准构建PUT。
            for (int i = 0; i < columnNameList.size(); ++i) {
                String qualifier = (String) columnNameList.get(i);
                if(qualifier == rowKeyCol){
                    continue;
                }
                wrapper = record.get(qualifier);
                // 如果没有这列值，则meaningfulSeq不加
                if (wrapper == null) {
                    continue;
                }
                constructPut(put, wrapper, qualifier);
            }

        }
        return put;
    }

    public void constructPut(Put put, BaseWrapper wrapper, String qualifier) throws Exception {
        // 优先从columnTypeMap中获取对应的DataType，如果为null，则从wrapper中获取。
        DataType dt = (DataType) columnTypeMap.get(qualifier);
        if(dt==null){
            dt = wrapper.getType();
        }
        WrapperSetter wrapperSetter = getWrapperSetter(dt);
        byte[] value = wrapperSetter.set(wrapper, null, null, 0);
        put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value);
    }

    @Override
    protected void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException {

        recordList.add(value);
        if (recordList.size() >= putBatchSize) {
            execUpdate();
        }
    }

    @Override
    protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException {
        recordList.add(value);
        if (recordList.size() >= putBatchSize) {
            execUpdate();
        }
    }

    @Override
    protected WrapperSetter getIntegerSetter() {
        return new WrapperSetter() {
            @Override
            public byte[] set(@NonNull BaseWrapper wrapper, Object row, String name, int seq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                return res.toByteArray();
            }
        };
    }

    @Override
    protected WrapperSetter getDoubleSetter() {
        return new WrapperSetter() {
            @Override
            public byte[] set(@NonNull BaseWrapper wrapper, Object row, String name, int seq) throws Exception {
                Double res = wrapper.asDouble();
                return Bytes.toBytes(res);
            }
        };
    }

    @Override
    protected WrapperSetter getBooleanSetter() {
        return new WrapperSetter() {
            @Override
            public byte[] set(@NonNull BaseWrapper wrapper, Object row, String name, int seq) throws Exception {
                Boolean res = wrapper.asBoolean();
                return Bytes.toBytes(res);
            }
        };
    }

    @Override
    protected WrapperSetter getStringSetter() {
        return new WrapperSetter() {
            @Override
            public byte[] set(@NonNull BaseWrapper wrapper, Object row, String name, int seq) throws Exception {
                String res = wrapper.asString();
                return Bytes.toBytes(res);
            }
        };
    }

    @Override
    protected WrapperSetter getDateSetter() {
        return new WrapperSetter() {
            @Override
            public byte[] set(@NonNull BaseWrapper wrapper, Object row, String name, int seq) throws Exception {
                return wrapper.asBytes();
            }
        };
    }

    @Override
    protected WrapperSetter getBytesSetter() {
        return new WrapperSetter() {
            @Override
            public byte[] set(@NonNull BaseWrapper wrapper, Object row, String name, int seq) throws Exception {
                byte[] res = wrapper.asBytes();
                return res;
            }
        };
    }

    @Override
    protected WrapperSetter getNullSetter() {
        return null;
    }

    @Override
    protected boolean isColumnNameOneLevel() {
        return false;
    }
}