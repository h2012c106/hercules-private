package com.xiaohongshu.db.hercules.hbase.mr;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.NullWrapper;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseOutputFormat extends HerculesOutputFormat implements HBaseManagerInitializer {

    /**
     * 配置 conf 并返回 HerculesRecordWriter
     */
    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        GenericOptions targetOptions = options.getTargetOptions();
        HBaseManager manager = initializeManager(targetOptions);
        HBaseManager.setTargetConf(manager.getConf(), targetOptions);
        // 传到 recordWriter 的 manager 有设置好了的 conf， 可以通过 manager 获得 Connection
        return new HBaseRecordWriter(manager, context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context){
        return new NullOutputCommitter();
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }
}

class HBaseRecordWriter extends HerculesRecordWriter<Put> {

    private static final Log LOG = LogFactory.getLog(HBaseRecordWriter.class);
    private final String columnFamily;
    private final String rowKeyCol;
    private final HBaseManager manager;

    private final BufferedMutator mutator;

    /**
     * 获取 columnFamily，rowKeyCol 并通过 conf
     */
    public HBaseRecordWriter(HBaseManager manager, TaskAttemptContext context) throws IOException {
        super(context, new HBaseOutputWrapperManager());
        this.manager = manager;
        Configuration conf = manager.getConf();
        columnFamily = conf.get(HBaseOutputOptionsConf.COLUMN_FAMILY);
        rowKeyCol = conf.get(HBaseOptionsConf.ROW_KEY_COL_NAME);
        List<String> temp = new ArrayList<>(columnNameList);
        temp.remove(rowKeyCol);
        columnNameList = temp;
        // 目前相关的变量统一从 Configuration conf 中拿取
        mutator = HBaseManager.getBufferedMutator(manager.getConf(), manager);
//        debug = options.getTargetOptions().getBoolean(HBaseOptionsConf.DEBUG, false);
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
        Put put = new Put(Bytes.toBytes(record.get(rowKeyCol).asString()));
        BaseWrapper wrapper;
        if(columnNameList.size()==0){
            for(Map.Entry<String, BaseWrapper> colVal: record.getRow().entrySet()){
                String qualifier = colVal.getKey();
                wrapper = colVal.getValue();
                constructPut(put, wrapper, qualifier);
            }
        }else{
            // 如果存在 columnNameList， 则以 columnNameList 为准构建PUT。
            for (String qualifier : columnNameList) {
                wrapper = record.get(qualifier);
                // 如果没有这列值，则meaningfulSeq不加
                constructPut(put, wrapper, qualifier);
            }
        }
        return put;
    }

    /**
     * 获取正确的 DataType 和 WrapperSetter，并将数据放入 Put
     */
    public void constructPut(Put put, BaseWrapper<?> wrapper, String qualifier) throws Exception {

        if (wrapper == null || wrapper instanceof NullWrapper) {
            LOG.info("No wrapper found for column: " + qualifier);
            return;
        }
        if(qualifier.equals(rowKeyCol)){
            // if the qualifier is the row key col, dont put it the the Put object
            return;
        }
        // 优先从columnTypeMap中获取对应的DataType，如果为null，则从wrapper中获取。
        DataType dt = columnTypeMap.get(qualifier);
        if(dt==null){
            dt = wrapper.getType();
        }
        WrapperSetter<Put> wrapperSetter = getWrapperSetter(dt);
        wrapperSetter.set(wrapper, put, columnFamily, qualifier, 0);
    }


    /**
     * innerColumnWrite 和 innerMapWrite 一致。因为 hbase 写入是遍历 HerculesWritable 中的 map
     */
    @Override
    protected void innerColumnWrite(HerculesWritable record){
        innerMapWrite(record);
    }

    @SneakyThrows
    @Override
    protected void innerMapWrite(HerculesWritable record) {
        Put put = generatePut(record);
        mutator.mutate(put);
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException {
        mutator.flush();
        manager.closeConnection();
    }
}