package com.xiaohongshu.db.hercules.kafka.mr;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.converter.blank.BlankKvConverterSupplier;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.option.KvOptionsConf;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class KafkaOutPutFormat extends HerculesOutputFormat implements KafkaManagerInitializer {

    @SneakyThrows
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
    private final KvConverterSupplier kvConverterSupplier;

    public KafkaRecordWriter(KafkaManager manager, TaskAttemptContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        super(context, new KafkaOutputWrapperManager());
        this.targetOptions = options.getTargetOptions();
        this.manager = manager;
        this.kvConverterSupplier = (KvConverterSupplier) Class.forName(options.getTargetOptions().getString(KvOptionsConf.SUPPLIER, "")).newInstance();
        if (this.kvConverterSupplier instanceof BlankKvConverterSupplier){
            throw new RuntimeException("BlankKvConverterSupplier is not supported in kafka writer.");
        }
    }

    // 若给定columnType，则以给定的为准，否则以wrapper的DataType为准。
    @Override
    protected void innerColumnWrite(HerculesWritable value) {
        manager.send("", kvConverterSupplier.getKvConverter().generateValue(value, targetOptions, columnTypeMap, columnNameList));
    }

    @Override
    protected void innerMapWrite(HerculesWritable value) {
        innerColumnWrite(value);
    }

    @Override
    protected void innerClose(TaskAttemptContext context) {
        manager.close();
    }
}
