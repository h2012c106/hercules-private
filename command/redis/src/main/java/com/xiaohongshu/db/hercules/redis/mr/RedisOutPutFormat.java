package com.xiaohongshu.db.hercules.redis.mr;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesKvRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import com.xiaohongshu.db.hercules.redis.option.RedisOptionConf;
import com.xiaohongshu.db.hercules.redis.schema.manager.RedisManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;
import static com.xiaohongshu.db.hercules.redis.RedisKV.KEY_SEQ;
import static com.xiaohongshu.db.hercules.redis.RedisKV.SCORE_SEQ;
import static com.xiaohongshu.db.hercules.redis.RedisKV.VALUE_SEQ;


public class RedisOutPutFormat extends HerculesOutputFormat<RedisKV> {

    @Override
    protected HerculesRecordWriter<RedisKV> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new RedisRecordWriter(context);
    }

    @Override
    protected WrapperSetterFactory<RedisKV> createWrapperSetterFactory() {
        return new RedisOutputWrapperManager();
    }

}

class RedisRecordWriter extends HerculesRecordWriter<RedisKV> {

    private static final Log LOG = LogFactory.getLog(RedisRecordWriter.class);

    @Assembly
    private final RedisManager manager = null;

    @Options(type = OptionsType.TARGET)
    private final GenericOptions targetOptions = null;

    @SchemaInfo
    private Schema schema;

    private List<String> strategyList = null;

    private int retry_count = 0;

    public RedisRecordWriter(TaskAttemptContext context) {
        super(context);
    }

    @Override
    protected final void innerWrite(HerculesWritable value) throws IOException, InterruptedException {

        String keyName = targetOptions.getString(KEY_NAME, null);
        String valueName = targetOptions.getString(VALUE_NAME, null);

        BaseWrapper<?> keyValue = value.get(keyName);
        BaseWrapper<?> valueValue = value.get(valueName);

        if (keyValue == null || valueValue == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Meaningless row: %s", value.toString()));
            }
        } else {
            if(value.getWriteStrategyList().size() > 0){
                strategyList = value.getWriteStrategyList();
            } else {
                LOG.warn(" kv strategy is null");
            }
            RedisKV kv = new RedisKV();
            try {

                if(targetOptions.getString(RedisOptionConf.REDIS_WRITE_TPYE, null).equalsIgnoreCase("zset")){
                    String scoreName = targetOptions.getString(RedisOptionConf.REDIS_SCORE_NAME, null);
                    BaseWrapper<?> scoreValue = value.get(scoreName);
                    if(scoreValue == null){
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("Meaningless row: %s", value.toString()));
                        }
                    } else {
                        getWrapperSetter(schema.getColumnTypeMap().getOrDefault(scoreName, scoreValue.getType()))
                                .set(scoreValue, kv, null, null, SCORE_SEQ);
                    }
                }

                getWrapperSetter(schema.getColumnTypeMap().getOrDefault(keyName, keyValue.getType()))
                        .set(keyValue, kv, null, null, KEY_SEQ);
                getWrapperSetter(schema.getColumnTypeMap().getOrDefault(valueName, valueValue.getType()))
                        .set(valueValue, kv, null, null, VALUE_SEQ);
            } catch (Exception e) {
                throw new IOException(e);
            }
            int retry_count = 0;
            boolean write_success = false;
            while (!write_success){
                try{
                    write_success = write(kv);
                } catch (Exception ex){
                    LOG.error(" write redis error:", ex);
                    retry_count++;
                    if(retry_count > 3) {
                        throw new IOException(" write redis failed.");
                    } else {
                        manager.acquireNewRedisSource();//连接池获取一个新的连接
                        Thread.sleep(30 * 1000);
                    }
                }
            }
        }
    }

    private boolean write(RedisKV kv) throws InterruptedException{
        if(strategyList != null) {
            manager.act(kv, strategyList);
        } else
            manager.set(kv);
        return true;
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






