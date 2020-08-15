package com.xiaohongshu.db.hercules.kafka.mr;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesKvRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.kafka.KafkaKV;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;
import static com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat.KEY_SEQ;
import static com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat.VALUE_SEQ;

public class KafkaOutPutFormat extends HerculesOutputFormat<KafkaKV> {

    public static final int KEY_SEQ = 0;
    public static final int VALUE_SEQ = 1;

    @Override
    protected HerculesRecordWriter<KafkaKV> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new KafkaRecordWriter(context);
    }

    @Override
    protected WrapperSetterFactory<KafkaKV> createWrapperSetterFactory() {
        return new KafkaOutputWrapperManager();
    }
}

class KafkaRecordWriter extends HerculesKvRecordWriter<KafkaKV> {

    private static final Log LOG = LogFactory.getLog(KafkaRecordWriter.class);

    @GeneralAssembly
    private final KafkaManager manager = null;

    @Options(type = OptionsType.TARGET)
    private final GenericOptions targetOptions = null;

    @SchemaInfo
    private Schema schema;

    public KafkaRecordWriter(TaskAttemptContext context) {
        super(context);
    }

    @Override
    protected void innerWriteKV(BaseWrapper<?> key, BaseWrapper<?> value) throws IOException, InterruptedException {
        String keyName = targetOptions.getString(KEY_NAME, null);
        String valueName = targetOptions.getString(VALUE_NAME, null);

        KafkaKV kv = new KafkaKV();
        try {
            getWrapperSetter(schema.getColumnTypeMap().getOrDefault(keyName, key.getType()))
                    .set(key, kv, null, null, KEY_SEQ);
            getWrapperSetter(schema.getColumnTypeMap().getOrDefault(valueName, value.getType()))
                    .set(value, kv, null, null, VALUE_SEQ);
        } catch (Exception e) {
            throw new IOException(e);
        }
        manager.send(kv);
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.IGNORE;
    }

    @Override
    protected void innerClose(TaskAttemptContext context) {
        manager.close();
    }
}
