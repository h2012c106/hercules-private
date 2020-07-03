package com.xiaohongshu.db.hercules.kafka.mr;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.kafka.option.KvOptionsConf;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.Charset;

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
    private Charset charset = Charset.defaultCharset();


    public KafkaRecordWriter(KafkaManager manager, TaskAttemptContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        super(context, new KafkaOutputWrapperManager());
        this.targetOptions = options.getTargetOptions();
        this.manager = manager;
        this.kvConverterSupplier = (KvConverterSupplier) Class.forName(options.getTargetOptions().getString(KvOptionsConf.SUPPLIER, "")).newInstance();
    }

    @Override
    protected void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException {
        manager.send("", kvConverterSupplier.getKvConverter().generateCanalEntry(value, targetOptions));
    }



    @Override
    protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException {
        innerColumnWrite(value);
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        manager.close();
    }
}
