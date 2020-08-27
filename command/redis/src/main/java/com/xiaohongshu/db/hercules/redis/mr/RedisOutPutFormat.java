package com.xiaohongshu.db.hercules.redis.mr;

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
import com.xiaohongshu.db.hercules.redis.RedisKV;
import com.xiaohongshu.db.hercules.redis.option.RedisOptionConf;
import com.xiaohongshu.db.hercules.redis.schema.manager.RedisManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;
import static com.xiaohongshu.db.hercules.redis.RedisKV.KEY_SEQ;
import static com.xiaohongshu.db.hercules.redis.RedisKV.VALUE_SEQ;


public class RedisOutPutFormat extends HerculesOutputFormat<RedisKV>  {

    @Override
    protected HerculesRecordWriter<RedisKV> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new RedisRecordWriter(context);
    }

    @Override
    protected WrapperSetterFactory<RedisKV> createWrapperSetterFactory() {
        return new RedisOutputWrapperManager();
    }

}

class RedisRecordWriter extends HerculesKvRecordWriter<RedisKV> {

    private static final Log LOG = LogFactory.getLog(RedisRecordWriter.class);

    @GeneralAssembly
    private final RedisManager manager = null;

    @Options(type = OptionsType.TARGET)
    private final GenericOptions targetOptions = null;

    @SchemaInfo
    private Schema schema;

    public RedisRecordWriter(TaskAttemptContext context) {
        super(context);
    }

    @Override
    protected void innerWriteKV(BaseWrapper<?> key, BaseWrapper<?> value) throws IOException, InterruptedException {
        String keyName = targetOptions.getString(KEY_NAME, null);
        String valueName = targetOptions.getString(VALUE_NAME, null);

        RedisKV kv = new RedisKV();
        try {
            getWrapperSetter(schema.getColumnTypeMap().getOrDefault(keyName, key.getType()))
                    .set(key, kv, null, null, KEY_SEQ);
            getWrapperSetter(schema.getColumnTypeMap().getOrDefault(valueName, value.getType()))
                    .set(value, kv, null, null, VALUE_SEQ);
        } catch (Exception e) {
            throw new IOException(e);
        }
        manager.set(kv);
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






