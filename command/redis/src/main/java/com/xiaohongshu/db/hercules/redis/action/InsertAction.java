package com.xiaohongshu.db.hercules.redis.action;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import redis.clients.jedis.Pipeline;

import java.util.Map;

/**
 * Created by jamesqq on 2020/9/19.
 */
public class InsertAction implements WriteAction{

    public void act(Pipeline pipeline, RedisKV kv, int expire) {
        String key = String.valueOf(kv.getKey().getValue());
        RedisKV.RedisKVValue value = kv.getValue();
        if (key == null || value.getValue() == null) {
            return;
        }
        switch ((BaseDataType) value.getDataType()) {
            case STRING:
                String stringValue = String.valueOf(value.getValue());
                pipeline.set(key, stringValue);
                break;
            case LIST:
                throw new RuntimeException("The type List is not supported by redis.");
            case MAP:
                Map<String, String> mapValue = (Map<String, String>) (value.getValue());
                pipeline.hmset(key, mapValue);
                break;
            default:
                throw new RuntimeException(String.format("The type [%s] is not supported by redis.", value.getDataType().toString()));
        }
    }
}