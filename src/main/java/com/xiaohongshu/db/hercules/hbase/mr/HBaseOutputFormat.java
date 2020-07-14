package com.xiaohongshu.db.hercules.hbase.mr;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.converter.blank.BlankKvConverterSupplier;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;
import com.xiaohongshu.db.hercules.core.option.KvOptionsConf;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HBaseOutputFormat extends HerculesOutputFormat implements HBaseManagerInitializer {

    /**
     * 配置 conf 并返回 HerculesRecordWriter
     */
    @SneakyThrows
    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) {

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
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
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
    private final KvConverterSupplier kvConverterSupplier;
    private final BufferedMutator mutator;

    public HBaseRecordWriter(HBaseManager manager, TaskAttemptContext context) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        super(context, new HBaseOutputWrapperManager());
        this.manager = manager;
        Configuration conf = manager.getConf();
        columnFamily = conf.get(HBaseOutputOptionsConf.COLUMN_FAMILY);
        rowKeyCol = conf.get(HBaseOptionsConf.ROW_KEY_COL_NAME);
        mutator = HBaseManager.getBufferedMutator(manager.getConf(), manager);
        kvConverterSupplier = (KvConverterSupplier) Class.forName(options.getTargetOptions().getString(KvOptionsConf.SUPPLIER, "")).newInstance();
        List<String> temp = new ArrayList<>(columnNameList);
        temp.remove(rowKeyCol);
        columnNameList = temp;
        if (columnNameList.size() == 0) {
            throw new RuntimeException("Column name list failed to fetch(no column name found).");
        }
        if (!options.getSourceOptions().getString(KvOptionsConf.SUPPLIER, "").contains("BlankKvConverterSupplier")) {
            columnNameList.clear();
            columnNameList.addAll(columnTypeMap.keySet());
        }
    }

    /**
     * @param record 根据是否提供不为空的columnNameList来生成PUT
     */
    public Put generatePut(HerculesWritable record) throws Exception {

        // TODO 更新row_key, row key 来自上游，可能存储在value里面, 暂时不支持 compositeRowKeyCol
        // 如果没找到row key col对应的值，抛错！说明用户没有定义好，或者某行数据不存在row key col对应的值。
        if (record.get(rowKeyCol) == null) {
            throw new RuntimeException("Row key col not found in the HerculesWritable object.");
        }
        Put put = new Put(Bytes.toBytes(record.get(rowKeyCol).asString()));
        BaseWrapper wrapper;
        if (columnNameList.size() == 0) {
            for (Map.Entry<String, BaseWrapper> colVal : record.getRow().entrySet()) {
                String qualifier = colVal.getKey();
                wrapper = colVal.getValue();
                constructPut(put, wrapper, qualifier);
            }
        } else {
            // 注意，columnNameList 可能可以确保不为空？
            // 如果存在 columnNameList， 则以 columnNameList 为准构建PUT。
            for (String qualifier : columnNameList) {
                wrapper = record.get(qualifier);
                constructPut(put, wrapper, qualifier);
            }
        }
        return put;
    }

    /**
     * 获取正确的 DataType 和 WrapperSetter，并将数据放入 Put
     */
    public void constructPut(Put put, BaseWrapper<?> wrapper, String qualifier) throws Exception {

        if (wrapper.isNull()) {
            if (LOG.isDebugEnabled()) {
                LOG.info("No wrapper found for column: " + qualifier);
            }
            return;
        }
        if (qualifier.equals(rowKeyCol)) {
            // if the qualifier is the row key col, don't put it into the Put object
            return;
        }
        // 优先从columnTypeMap中获取对应的DataType，如果为null，则从wrapper中获取。
        DataType dt = columnTypeMap.get(qualifier);
        if (dt == null) {
            dt = wrapper.getType();
        }
        getWrapperSetter(dt).set(wrapper, put, columnFamily, qualifier, 0);
    }

    /**
     * innerColumnWrite 和 innerMapWrite 处理逻辑暂设一致。hbase 写入是遍历 HerculesWritable 中的 map
     */
    @Override
    protected void innerColumnWrite(HerculesWritable record) throws IOException {
        innerMapWrite(record);
    }

    @Override
    protected void innerMapWrite(HerculesWritable record) throws IOException {
        Put put;

        try {
            if (kvConverterSupplier instanceof BlankKvConverterSupplier) {
                put = generatePut(record);
            } else {
                put = getConvertedPut(record);
            }
            mutator.mutate(put);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected Put getConvertedPut(HerculesWritable record) {
        String qualifier = options.getTargetOptions().getString(HBaseOutputOptionsConf.CONVERT_COLUMN_NAME, "");
        Put put = new Put(Bytes.toBytes(record.get(rowKeyCol).asString()));
        WritableUtils.remove(record.getRow(), Collections.singletonList(rowKeyCol));
        byte[] value = kvConverterSupplier.getKvConverter().generateValue(record, options.getTargetOptions(), columnTypeMap, columnNameList);
        put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value);
        return put;
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException{
        try {
            mutator.close();
            manager.closeConnection();
        } catch (IOException e) {
            throw new IOException();
        }
    }
}