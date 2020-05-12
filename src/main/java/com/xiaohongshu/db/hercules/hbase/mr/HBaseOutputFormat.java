package com.xiaohongshu.db.hercules.hbase.mr;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.serialize.datatype.NullWrapper;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataType;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
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
    private Map<String, HBaseDataType> hbaseColumnTypeMap;

    /**
     * 配置 conf 并返回 HerculesRecordWriter
     */
    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        targetOptions = options.getTargetOptions();
        hbaseColumnTypeMap = HBaseDataTypeConverter.convert(targetOptions.getJson(HBaseOptionsConf.HBASE_COLUMN_TYPE_MAP, null));
        manager = initializeManager(targetOptions);
        HBaseManager.setTargetConf(manager.getConf(), targetOptions);
        // 传到 recordWriter 的 manager 有设置好了的 conf， 可以通过 manager 获得 Connection
        return new HBaseRecordWriter(manager, context, hbaseColumnTypeMap);
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

class HBaseRecordWriter extends HerculesRecordWriter<Put> {

    private static final Log LOG = LogFactory.getLog(HBaseRecordWriter.class);
    private final String columnFamily;
    private final String rowKeyCol;
    private final HBaseManager manager;
    private final Map<String, HBaseDataType> hbaseColumnTypeMap;

    private final BufferedMutator mutator;
    private final boolean debug;

    /**
     * 获取 columnFamily，rowKeyCol 并通过 conf
     */
    public HBaseRecordWriter(HBaseManager manager, TaskAttemptContext context, Map<String, HBaseDataType> hbaseColumnTypeMap) throws IOException {
        super(context);
        this.manager = manager;
        this.hbaseColumnTypeMap = hbaseColumnTypeMap;
        Configuration conf = manager.getConf();
        columnFamily = conf.get(HBaseOutputOptionsConf.COLUMN_FAMILY);
        rowKeyCol = conf.get(HBaseOutputOptionsConf.ROW_KEY_COL_NAME);
        // 目前相关的变量统一从 Configuration conf 中拿取
        mutator = HBaseManager.getBufferedMutator(manager.getConf(), manager);
//        debug = options.getTargetOptions().getBoolean(HBaseOptionsConf.DEBUG, false);
        debug = true;
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
    public void constructPut(Put put, BaseWrapper wrapper, String qualifier) throws Exception {

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
        if(debug){
            LOG.info("COLUMN NAME: "+qualifier);
        }
        wrapperSetter.set(wrapper, put, qualifier, 0);
    }


    /**
     * innerColumnWrite 和 innerMapWrite 一致。因为 hbase 写入是遍历 HerculesWritable 中的 map
     */
    @Override
    protected void innerColumnWrite(HerculesWritable record) throws IOException, InterruptedException {
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
    protected WrapperSetter<Put> getIntegerSetter() {
        return (wrapper, put, name, seq) -> {
            Long res = wrapper.asLong();
            if(debug){
                LOG.info("GOT INTEGER DATA: "+res);
            }
            HBaseDataType dataType = hbaseColumnTypeMap.get(name);
            switch(dataType){
                case SHORT:
                    put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res.shortValue()));
                    break;
                case INT:
                    put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res.intValue()));
                    break;
                case LONG:
                    put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res));
                    break;
                default:
                    throw new MapReduceException("Unknown data type: " + dataType.name());
            }
        };
    }

    @Override
    protected WrapperSetter<Put> getDoubleSetter() {
        return (wrapper, put, name, seq) -> {
            HBaseDataType dataType = hbaseColumnTypeMap.get(name);
            if(debug){
                LOG.info("GOT DOUBLE DATA: "+wrapper.asDouble());
            }
            switch(dataType){
                case FLOAT:
                    put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asDouble().floatValue()));
                    break;
                case DOUBLE:
                    put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asDouble()));
                    break;
                case BIGDECIMAL:
                    put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asBigDecimal()));
                    break;
                default:
                    throw new MapReduceException("Unknown data type: " + dataType.name());
            }
        };
    }

    @Override
    protected WrapperSetter<Put> getBooleanSetter() {
        return (wrapper, put, name, seq) -> {
            Boolean res = wrapper.asBoolean();
            if(debug){
                LOG.info("GOT BOOLEAN DATA: "+res);
            }
            put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res));
        };
    }

    @Override
    protected WrapperSetter<Put> getStringSetter() {
        return (wrapper, put, name, seq) -> {
            String res = wrapper.asString()==null? "":wrapper.asString();
            if(debug){
                LOG.info("GOT STRING DATA: "+res);
            }
            put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res));
        };
    }

    @Override
    protected WrapperSetter<Put> getDateSetter() {
        return (wrapper, put, name, seq) -> put.addColumn(columnFamily.getBytes(), name.getBytes(), wrapper.asBytes());
    }

    @Override
    protected WrapperSetter<Put> getBytesSetter() {
        return (wrapper, put, name, seq) -> put.addColumn(columnFamily.getBytes(), name.getBytes(), wrapper.asBytes());
    }

    @Override
    protected WrapperSetter<Put> getNullSetter() {
        return null;
    }
}