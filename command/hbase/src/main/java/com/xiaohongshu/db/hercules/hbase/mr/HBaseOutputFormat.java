package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

public class HBaseOutputFormat extends HerculesOutputFormat<Put> {

    @GeneralAssembly
    private HBaseManager manager;

    /**
     * 配置 conf 并返回 HerculesRecordWriter
     */
    @SneakyThrows
    @Override
    public HerculesRecordWriter<Put> innerGetRecordWriter(TaskAttemptContext context) {
        return new HBaseRecordWriter(manager, context);
    }

    @Override
    protected WrapperSetterFactory<Put> createWrapperSetterFactory() {
        return new HBaseOutputWrapperManager();
    }
}

class HBaseRecordWriter extends HerculesRecordWriter<Put> {

    private static final Log LOG = LogFactory.getLog(HBaseRecordWriter.class);
    private String columnFamily;
    private String rowKeyCol;
    private final HBaseManager manager;
    //    private final BufferedMutator mutator;
    private Table table;
    private final List<Put> putsBuffer = new LinkedList<>();
    private final int PUT_BUFFER_SIZE = 10000;

    private List<String> columnNameList;
    private Map<String, DataType> columnTypeMap;

    private final Configuration configuration;

    @Options(type = OptionsType.TARGET)
    private GenericOptions options;

    @SchemaInfo
    private Schema schema;

    public HBaseRecordWriter(HBaseManager manager, TaskAttemptContext context) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        super(context);
        this.manager = manager;
        this.configuration = context.getConfiguration();
    }

    @SneakyThrows
    @Override
    protected void innerAfterInject() {
        columnFamily = options.getString(HBaseOutputOptionsConf.COLUMN_FAMILY, null);
        rowKeyCol = options.getString(HBaseOptionsConf.ROW_KEY_COL_NAME, null);
//        mutator = HBaseManager.getBufferedMutator(manager.getConf(), manager);
        table = HBaseManager.getTable(options.getString(HBaseOptionsConf.TABLE, null), manager.getConnection(configuration));
        columnNameList = schema.getColumnNameList();
        columnTypeMap = schema.getColumnTypeMap();

        List<String> temp = new ArrayList<>(columnNameList);
        temp.remove(rowKeyCol);
        columnNameList = temp;
        if (columnNameList.size() == 0) {
            throw new RuntimeException("Column name list failed to fetch(no column name found).");
        }
//        if (!options.getString(KvOptionsConf.SUPPLIER, "").contains("BlankKvConverterSupplier")) {
//            columnNameList.clear();
//            columnNameList.addAll(columnTypeMap.keySet());
//        }
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
        BaseWrapper<?> wrapper;
        if (columnNameList.size() == 0) {
            for (Map.Entry<String, BaseWrapper<?>> colVal : record.getRow().entrySet()) {
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

    @Override
    protected void innerWrite(HerculesWritable record) throws IOException {
        Put put;

        try {
            put = generatePut(record);
//            mutator.mutate(put);
            put.setDurability(Durability.SKIP_WAL);
            putsBuffer.add(put);
            if (putsBuffer.size() > PUT_BUFFER_SIZE) {
                table.batch(putsBuffer, null);
//                table.put(putsBuffer);
                putsBuffer.clear();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.IGNORE;
    }

//    protected Put getConvertedPut(HerculesWritable record) {
//        String qualifier = options.getString(HBaseOutputOptionsConf.CONVERT_COLUMN_NAME, "");
//        Put put = new Put(Bytes.toBytes(record.get(rowKeyCol).asString()));
//        WritableUtils.remove(record.getRow(), Collections.singletonList(rowKeyCol));
//        byte[] value = kvSerializerSupplier.getKvSerializer().generateValue(record, options.getTargetOptions(), columnTypeMap, columnNameList);
//        put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value);
//        return put;
//    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException {
        try {
//            mutator.close();
            table.batch(putsBuffer, null);
            table.close();
            manager.closeConnection();
        } catch (IOException | InterruptedException e) {
            throw new IOException();
        }
    }
}