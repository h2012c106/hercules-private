package com.xiaohongshu.db.hercules.kafka.mr;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOutputOptionConf;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Map;

public class KafkaOutPutFormat extends HerculesOutputFormat implements KafkaManagerInitializer {

    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) {
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        GenericOptions targetOptions = options.getTargetOptions();
        KafkaManager manager = initializeManager(targetOptions);
        return new KafkaRecordWriter(manager, context);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }

    @Override
    public KafkaManager initializeManager(GenericOptions options) {
        return new KafkaManager(options);
    }
}

class KafkaRecordWriter extends HerculesRecordWriter<CanalEntry.Entry> {

    private static final Log LOG = LogFactory.getLog(KafkaRecordWriter.class);
    private final KafkaManager manager;
    private final GenericOptions targetOptions;
    private Charset charset = Charset.defaultCharset();


    public KafkaRecordWriter(KafkaManager manager, TaskAttemptContext context) {
        super(context, new KafkaOutputWrapperManager());
        this.targetOptions = options.getTargetOptions();
        this.manager = manager;
    }

    @Override
    protected void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException {
        manager.send("", generateCanalEntry(value).toByteArray());
    }

    public CanalEntry.Entry generateCanalEntry(HerculesWritable value) {
        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setSourceType(CanalEntry.Type.MYSQL);
        headerBuilder.setSchemaName(targetOptions.getString(KafkaOutputOptionConf.SCHEMA_NAME, ""));
        headerBuilder.setTableName(targetOptions.getString(KafkaOutputOptionConf.TABLE_NAME, ""));

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        String keyCol = targetOptions.getString(KafkaOutputOptionConf.KEY, "");

        for (Map.Entry<String, BaseWrapper> entry : value.entrySet()) {
            BaseWrapper wrapper = entry.getValue();
            DataType type = wrapper.getType();
            String columnName = entry.getKey();
            String columnValue = convertValue(wrapper);

            CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder()
                    .setName(columnName)
                    .setSqlType(getColumnType(type))
                    .setIsKey(columnName.equals(keyCol))
                    .setIsNull(columnValue == null);
            if (columnValue != null) {
                columnBuilder.setValue(columnValue);
            }
            CanalEntry.Column column = columnBuilder.build();
            rowDataBuilder.addAfterColumns(column);
        }
        rowChangeBuilder.addRowDatas(rowDataBuilder.build());
        return CanalEntry.Entry.newBuilder()
                .setHeader(headerBuilder.build())
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setStoreValue(rowChangeBuilder.build().toByteString())
                .build();
    }

    @Override
    protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException {
        innerColumnWrite(value);
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        manager.close();
    }

    private String convertValue(BaseWrapper wrapper) {
        BaseDataType type = wrapper.getType().getBaseDataType();
        switch (type) {
            case BYTES:
                return new String(wrapper.asBytes(), StandardCharsets.ISO_8859_1);
            case BYTE:
                return String.valueOf(wrapper.asBigInteger().byteValueExact());
            case SHORT:
                return String.valueOf(wrapper.asBigInteger().shortValueExact());
            case INTEGER:
                return String.valueOf(wrapper.asBigInteger().intValueExact());
            case LONG:
                return String.valueOf(wrapper.asBigInteger().longValueExact());
            case FLOAT:
                return String.valueOf(OverflowUtils.numberToFloat(wrapper.asBigDecimal()));
            case DOUBLE:
                return String.valueOf(OverflowUtils.numberToDouble(wrapper.asBigDecimal()));
            case DECIMAL:
                return String.valueOf(wrapper.asBigDecimal());
            case BOOLEAN:
                return String.valueOf(wrapper.asBoolean());
            case STRING:
            case DATE:
            case DATETIME:
                return wrapper.asString();
            case TIME:
                return wrapper.asDate().toString();
            default:
                throw new RuntimeException("Unknown column type: " + type.getBaseDataType().name());
        }
    }

    private int getColumnType(DataType type) {
        switch (type.getBaseDataType()) {
            case BYTES:
                return Types.BINARY;
            case BYTE:
                return Types.TINYINT;
            case SHORT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case BOOLEAN:
                return Types.BOOLEAN;
            case STRING:
                return Types.VARCHAR;
            case DATE:
                return Types.DATE;
            case DATETIME:
                return Types.TIMESTAMP;
            case TIME:
                return Types.TIME;
            default:
                throw new RuntimeException("Unknown column type: " + type.getBaseDataType().name());
        }
    }
}
