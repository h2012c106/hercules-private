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
import lombok.SneakyThrows;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
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

    private GenericOptions targetOptions;
    private HBaseManager manager;

    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        targetOptions = options.getTargetOptions();
        manager = initializeManager(targetOptions);
        Configuration conf = manager.getConf();
        setConf(conf);
        String tableName = targetOptions.getString(HBaseOutputOptionsConf.OUTPU_TABLE, null);
        // 传到 recordWriter 的 manager 有设置好了的 conf， 可以直接 createConnection
        return new HBaseRecordWriter(manager, tableName, context);
    }

    // setup conf according to targetOptions
    private void setConf(Configuration conf){
        HBaseManager.setTargetConf(conf, targetOptions);
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

    private BufferedMutator mutator;


    public HBaseRecordWriter(HBaseManager manager, String tableName, TaskAttemptContext context) throws IOException {
        super(context);

        this.manager = manager;
        this.tableName = tableName;
        Configuration conf = manager.getConf();
        conn = manager.getConnection();
        columnFamily = conf.get(HBaseOutputOptionsConf.COLUMN_FAMILY);
        rowKeyCol = conf.get(HBaseOutputOptionsConf.ROW_KEY_COL_NAME);

        // 目前相关的变量统一从 Configuration conf 中拿取
        putBatchSize = conf.getLong(HBaseOutputOptionsConf.PUT_BATCH_SIZE, HBaseOutputOptionsConf.DEFAULT_PUT_BATCH_SIZE);
        recordList = new ArrayList<>(putBatchSize.intValue());
        mutator = HBaseManager.getBufferedMutator(manager.getConf(), manager);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        // TODO Elegantly terminate job
        manager.closeConnection();
    }

    /**
     * @param record
     * 根据是否提供不为空的columnNameList来生成PUT
     */
    public Put generatePut(HerculesWritable record) throws Exception {

        // TODO 更新row_key, row key 来自上游，可能存储在value里面, 暂时不支持 compositeRowKeyCol
        // 如果没找到row key col对应的值，抛错！说明用户没有定义好，或者某行数据不存在row key col对应的值。
        if(record.get(rowKeyCol)==null){
            throw new RuntimeException("Row key col not found in the HerculesWritable object.");
        }
        Put put = new Put(record.get(rowKeyCol).asBytes());
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
        if(qualifier==rowKeyCol){
            // if the qualifier is the row key col, dont put it the the Put object
            return;
        }
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
        innerMapWrite(value);
    }

    @SneakyThrows
    @Override
    protected void innerMapWrite(HerculesWritable record) {
        recordList.add(record);
        Put put = generatePut(record);
        mutator.mutate(put);
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