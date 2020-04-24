package com.xiaohongshu.db.hercules.hbase2;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.hbase2.MangerFactory.ManagerFactory;
import com.xiaohongshu.db.hercules.hbase2.option.HbaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.hbase2.schema.manager.HbaseManager;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSRecordWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class HbaseOutputFormat extends OutputFormat<NullWritable, HerculesWritable> {

    private Configuration conf;
    private GenericOptions targetOptions;
    private HbaseManager manager;

    @Override
    public RecordWriter<NullWritable, HerculesWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        targetOptions = options.getTargetOptions();
        manager = ManagerFactory.getManager(targetOptions, HbaseManager.class);
        conf = manager.getConf();
        setConf();
        String tableName = targetOptions.getString(HbaseOutputOptionsConf.OUTPU_TABLE, null);
        return new HbaseRecordWriter(manager, tableName);
    }

    // setup conf according to targetOptions
    private void setConf(){
        // TODO 完成conf的设置
        conf.set(HbaseOutputOptionsConf.COLUMN_FAMILY,
                targetOptions.getString(HbaseOutputOptionsConf.COLUMN_FAMILY,null));

        conf.setInt(HbaseOutputOptionsConf.EXECUTE_THREAD_NUM,
                targetOptions.getInteger(HbaseOutputOptionsConf.EXECUTE_THREAD_NUM, HbaseOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM));

        conf.setInt(HbaseOutputOptionsConf.PUT_BATCH_SIZE,
                targetOptions.getInteger(HbaseOutputOptionsConf.PUT_BATCH_SIZE,HbaseOutputOptionsConf.DEFAULT_PUT_BATCH_SIZE));

        conf.set(HbaseOutputOptionsConf.OUTPU_TABLE, targetOptions.getString(HbaseOutputOptionsConf.OUTPU_TABLE,null));

        conf.set(HbaseOutputOptionsConf.ROW_KEY_COL, targetOptions.getString(HbaseOutputOptionsConf.ROW_KEY_COL,null));

    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }
}

class HbaseRecordWriter extends RecordWriter<NullWritable, HerculesWritable> {

    private Connection conn;
    private String columnFamily;
    private String rowKeyCol;
    private String tableName;
    private List<HerculesWritable> recordList;
    private Long putBatchSize;
    private HbaseManager manager;
    private static final Log LOG = LogFactory.getLog(RDBMSRecordWriter.class);

    // ThreadPool
    private int threadNum;
    private ExecutorService threadPool;
    private AtomicBoolean threadPoolClosed = new AtomicBoolean(false);
    final private List<Exception> exceptionList = new ArrayList<Exception>();
    final private BlockingQueue<HbaseRecordWriter.WorkerMission> missionQueue = new SynchronousQueue<HbaseRecordWriter.WorkerMission>();

    public HbaseRecordWriter(HbaseManager manager, String tableName) throws IOException {

        this.manager = manager;
        this.tableName = tableName;
        Configuration conf = manager.getConf();
        conn = manager.getConnection();
        columnFamily = conf.get(HbaseOutputOptionsConf.COLUMN_FAMILY);
        rowKeyCol = conf.get(HbaseOutputOptionsConf.ROW_KEY_COL);

        // 目前相关的变量统一从 Configuration conf 中拿取
        putBatchSize = conf.getLong(HbaseOutputOptionsConf.PUT_BATCH_SIZE, HbaseOutputOptionsConf.DEFAULT_PUT_BATCH_SIZE);
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
        missionQueue.put(new HbaseRecordWriter.WorkerMission(copiedRecordList, false));
    }

    @Override
    public void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        recordList.add(value);
        if (recordList.size() >= putBatchSize) {
            execUpdate();
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        manager.closeConnection();
    }

    public void generateThreadPool(Configuration conf) throws IOException {


        threadNum = conf.getInt(HbaseOutputOptionsConf.EXECUTE_THREAD_NUM,
                HbaseOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM);
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
                        HbaseRecordWriter.WorkerMission mission = null;
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
                missionQueue.put(new HbaseRecordWriter.WorkerMission(null, true));
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

    public List<Put> generatePutList(List<HerculesWritable> herculesWritableList){

        List<Put> putList = new ArrayList<>();
        for(HerculesWritable value: herculesWritableList){
            putList.add(generatePut(value));
        }
        return putList;
    }

    public Put generatePut(HerculesWritable value){

        // TODO 更新row_key, row key 来自上游，可能存储在value里面
        Put put = new Put("row_key".getBytes());

        // take value as a map{String columnName: Wrapper}
        for(Map.Entry<String, BaseWrapper> s: value.getRow()){
            put.addColumn(columnFamily.getBytes(),s.getKey().getBytes(),s.getValue().asBytes());
        }
        return put;
    }
}
