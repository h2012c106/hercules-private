package com.xiaohongshu.db.hercules.kafka.mr;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.supplier.KvSerializerSupplier;
import com.xiaohongshu.db.hercules.converter.blank.BlankKvConverterSupplier;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.KvOptionsConf;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class KafkaOutPutFormat extends HerculesOutputFormat implements KafkaManagerInitializer {

    @SneakyThrows
    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) {
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        GenericOptions targetOptions = options.getTargetOptions();
        KafkaManager manager = initializeManager(targetOptions);
        KvSerializerSupplier kvSerializerSupplier = (KvSerializerSupplier) Class.forName(options.getTargetOptions().getString(KvOptionsConf.SUPPLIER, ""))
                .getConstructor(GenericOptions.class).newInstance(options.getTargetOptions());
        return new KafkaRecordWriter(manager, context, kvSerializerSupplier);
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
    private final KvSerializerSupplier kvSerializerSupplier;
    private final String kafkaKeyCol;


    public KafkaRecordWriter(KafkaManager manager, TaskAttemptContext context, KvSerializerSupplier kvSerializerSupplier) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        super(context, kvSerializerSupplier.getWrapperSetterFactory());
        this.targetOptions = options.getTargetOptions();
        this.kafkaKeyCol = this.targetOptions.getString(KafkaOptionConf.KAFKA_KEY, "");
        this.manager = manager;
        this.kvSerializerSupplier = kvSerializerSupplier;
        if (this.kvSerializerSupplier instanceof BlankKvConverterSupplier) {
            throw new RuntimeException("BlankKvConverterSupplier is not supported in kafka writer. Please specify a valid kvConverter.");
        }
    }

    // 若给定columnType，则以给定的为准，否则以wrapper的DataType为准。
    @Override
    protected void innerColumnWrite(HerculesWritable value) {
        String key = value.get(kafkaKeyCol).asString();
//        manager.send(Thread.currentThread().getName(), kvConverterSupplier.getKvConverter().generateValue(value, targetOptions, columnTypeMap, columnNameList));
        manager.send(key, kvSerializerSupplier.getKvSerializer().generateValue(value, targetOptions, columnTypeMap, columnNameList));
//        manager.send(kvConverterSupplier.getKvConverter().getKey(), kvConverterSupplier.getKvConverter().generateValue(value, targetOptions, columnTypeMap, columnNameList));
    }

    @Override
    protected void innerWrite(HerculesWritable value) {
        innerColumnWrite(value);
    }

    @Override
    protected void innerClose(TaskAttemptContext context) {
        manager.close();
    }
}
