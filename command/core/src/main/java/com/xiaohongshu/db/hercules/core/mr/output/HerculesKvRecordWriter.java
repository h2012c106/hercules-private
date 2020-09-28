package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;

public abstract class HerculesKvRecordWriter<T> extends HerculesRecordWriter<T> {

    private static final Log LOG = LogFactory.getLog(HerculesKvRecordWriter.class);

    @Options(type = OptionsType.TARGET)
    private GenericOptions options;

    private List<String> strategyList = null;

    private boolean flag = true;

    public HerculesKvRecordWriter(TaskAttemptContext context) {
        super(context);
    }

    abstract protected void innerWriteKV(BaseWrapper<?> key, BaseWrapper<?> value) throws IOException, InterruptedException;

    @Override
    protected final void innerWrite(HerculesWritable value) throws IOException, InterruptedException {
        if (!options.hasProperty(KEY_NAME) || !options.hasProperty(VALUE_NAME)) {
            // 用kv writer的说明一定是kv类型的数据源，你kv数据源不用kv options conf是不是不太合适
            throw new RuntimeException("If use kv writer, must use kv options.");
        }
        String keyName = options.getString(KEY_NAME, null);
        String valueName = options.getString(VALUE_NAME, null);
        BaseWrapper<?> keyValue = value.get(keyName);
        BaseWrapper<?> valueValue = value.get(valueName);
        if (keyValue == null || valueValue == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Meaningless row: %s", value.toString()));
            }
        } else {
            if(value.getWriteStrategyList().size() > 0){
                logForStrategy(value.getWriteStrategyList());
                strategyList = value.getWriteStrategyList();
            } else {
                if(flag)
                    LOG.warn(" kv strategy is null");
                flag = false;
            }
            innerWriteKV(keyValue, valueValue);
        }
    }

    private void logForStrategy(List<String> list){
        if(flag)
            LOG.warn(" kv strategyList is:" + list);
        flag = false;
    }

    public List<String> getStrategyList(){
        return strategyList;
    }

}
