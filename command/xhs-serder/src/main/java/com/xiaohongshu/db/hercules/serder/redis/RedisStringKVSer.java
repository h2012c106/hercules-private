package com.xiaohongshu.db.hercules.serder.redis;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.StringWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import com.xiaohongshu.db.hercules.redis.mr.RedisOutputWrapperManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jamesqq on 2020/11/3.
 */
public class RedisStringKVSer extends KVSer<RedisKV> implements InjectedClass {

    @Options(type = OptionsType.SER)
    private GenericOptions options;

    @SchemaInfo
    private Schema schema;

    private static final Log log = LogFactory.getLog(RedisStringKVSer.class);

    private RedisStringKVOutputOptionConf.StringKVFormat format;

    public RedisStringKVSer(){
        super(new RedisOutputWrapperManager());
    }

    @Override
    public void afterInject() {
        format = RedisStringKVOutputOptionConf.StringKVFormat.valueOfIgnoreCase(options.getString(RedisStringKVOutputOptionConf.FORMAT, null));
    }

    @Override
    protected BaseWrapper<?> writeValue(HerculesWritable in) throws IOException, InterruptedException {

        MapWrapper mapWrapper = in.getRow();

        switch (format) {
            case JSONSTRING:
                StringBuilder sBulider = new StringBuilder();
                for (Map.Entry<String, BaseWrapper<?>> entry : mapWrapper.entrySet()) {
                    BaseWrapper baseWrapper = entry.getValue();
                    sBulider.append(baseWrapper.asString());
                }
                return StringWrapper.get(sBulider.toString());
            case JSONMAP:
                try{
                    return StringWrapper.get(mapWrapper.asString());
                } catch (Exception e){
                    log.error(" row is:" + mapWrapper.toString());
                    throw e;
                }
            default:
                throw new RuntimeException("Unknown oplog format: " + format);
        }

    }
}