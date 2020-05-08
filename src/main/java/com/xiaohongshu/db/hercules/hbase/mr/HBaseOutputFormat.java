package com.xiaohongshu.db.hercules.hbase.mr;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
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
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseOutputFormat extends HerculesOutputFormat implements HBaseManagerInitializer {

    private GenericOptions targetOptions;
    private HBaseManager manager;

    /**
     * 配置 conf 并返回 HerculesRecordWriter
     */
    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        targetOptions = options.getTargetOptions();
        manager = initializeManager(targetOptions);
        HBaseManager.setTargetConf(manager.getConf(), targetOptions);
        // 传到 recordWriter 的 manager 有设置好了的 conf， 可以通过 manager 获得 Connection
        return new HBaseRecordWriter(manager, context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }
}

class HBaseRecordWriter extends HerculesRecordWriter<List<byte[]>> {

    private String columnFamily;
    private String rowKeyCol;
    private HBaseManager manager;
    private static final Log LOG = LogFactory.getLog(HBaseRecordWriter.class);

    private BufferedMutator mutator;

    /**
     * 获取 columnFamily，rowKeyCol 并通过 conf
     */
    public HBaseRecordWriter(HBaseManager manager, TaskAttemptContext context) throws IOException {
        super(context);

        this.manager = manager;
        Configuration conf = manager.getConf();
        columnFamily = conf.get(HBaseOutputOptionsConf.COLUMN_FAMILY);
        rowKeyCol = conf.get(HBaseOutputOptionsConf.ROW_KEY_COL_NAME);

        // 目前相关的变量统一从 Configuration conf 中拿取
        mutator = HBaseManager.getBufferedMutator(manager.getConf(), manager);
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
                wrapper = record.get(qualifier);
                // 如果没有这列值，则meaningfulSeq不加
                if (wrapper == null) {
                    LOG.info("No wrapper found for column: "+qualifier);
                    continue;
                }
                constructPut(put, wrapper, qualifier);
            }
        }
        LOG.info("TO PUT! "+put.toJSON());
        return put;
    }

    /**
     * 获取正确的 DataType 和 WrapperSetter，并将数据放入 Put
     * @param put
     * @param wrapper
     * @param qualifier
     */
    public void constructPut(Put put, BaseWrapper wrapper, String qualifier) throws Exception {
        if(qualifier.equals(rowKeyCol)){
            // if the qualifier is the row key col, dont put it the the Put object
            return;
        }
        // 优先从columnTypeMap中获取对应的DataType，如果为null，则从wrapper中获取。
        DataType dt = (DataType) columnTypeMap.get(qualifier);
        if(dt==null){
            dt = wrapper.getType();
        }
        WrapperSetter<List<byte[]>> wrapperSetter = getWrapperSetter(dt);
        List<byte[]> valueContainer = new ArrayList<>();
        wrapperSetter.set(wrapper, valueContainer, null, 0);
        byte[] value = valueContainer.get(0);
        put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value);
    }


    /**
     * innerColumnWrite 和 innerMapWrite 一致。因为 hbase 写入是遍历 HerculesWritable 中的 map
     */
    @Override
    protected void innerColumnWrite(HerculesWritable record) throws IOException, InterruptedException {

        LOG.info("TO RECORD!: "+record.toString());
        innerMapWrite(record);
    }

    @SneakyThrows
    @Override
    protected void innerMapWrite(HerculesWritable record) {
        Put put = generatePut(record);
        mutator.mutate(put);
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        mutator.flush();
        manager.closeConnection();
    }

    @Override
    protected WrapperSetter<List<byte[]>> getIntegerSetter() {
        return new WrapperSetter<List<byte[]>>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, List<byte[]> row, String name, int seq) throws Exception {
                Long res = wrapper.asLong();
                row.add(Bytes.toBytes(res));
            }
        };
    }

    @Override
    protected WrapperSetter<List<byte[]>> getDoubleSetter() {
        return new WrapperSetter<List<byte[]>>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, List<byte[]> row, String name, int seq) throws Exception {
                Double res = wrapper.asDouble();
                row.add(Bytes.toBytes(res));
            }
        };
    }

    @Override
    protected WrapperSetter<List<byte[]>> getBooleanSetter() {
        return new WrapperSetter<List<byte[]>>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, List<byte[]> row, String name, int seq) throws Exception {
                Boolean res = wrapper.asBoolean();
                row.add(Bytes.toBytes(res));
            }
        };
    }

    @Override
    protected WrapperSetter<List<byte[]>> getStringSetter() {
        return new WrapperSetter<List<byte[]>>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, List<byte[]> row, String name, int seq) throws Exception {
                String res = wrapper.asString();
                row.add(Bytes.toBytes(res));
            }
        };
    }

    @Override
    protected WrapperSetter<List<byte[]>> getDateSetter() {
        return new WrapperSetter<List<byte[]>>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, List<byte[]> row, String name, int seq) throws Exception {
                row.add(wrapper.asBytes());
            }
        };
    }

    @Override
    protected WrapperSetter<List<byte[]>> getBytesSetter() {
        return new WrapperSetter<List<byte[]>>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, List<byte[]> row, String name, int seq) throws Exception {
                row.add(wrapper.asBytes());
            }
        };
    }

    @Override
    protected WrapperSetter<List<byte[]>> getNullSetter() {
        return null;
    }
}