package com.xiaohongshu.db.hercules.redis.action;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.util.SafeEncoder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jamesqq on 2020/9/19.
 */
public class InsertAction implements WriteAction{

    private static final Log LOG = LogFactory.getLog(InsertAction.class);

    public void act(Pipeline pipeline, RedisKV kv, Integer expire, String writeType) {
        String key = String.valueOf(kv.getKey().getValue());
        RedisKV.RedisKVValue value = kv.getValue();
        if (key == null || value.getValue() == null) {
            return;
        }
        switch (writeType.toLowerCase()) {
            case "string":
                String stringValue = String.valueOf(value.getValue());
                pipeline.set(key, stringValue);
                break;
            case "list":
                byte[] key_byte = SafeEncoder.encode(key);
                byte[] value_byte = SafeEncoder.encode(String.valueOf(value.getValue()));
                pipeline.rpush(key_byte, value_byte);
                break;
            case "hash":
                byte[] key_byte_hash = SafeEncoder.encode(key);
                Map<byte[], byte[]> mapValue = (Map<byte[], byte[]>) (value.getValue());
                pipeline.hmset(key_byte_hash, mapValue);
                break;
            case "set":
                byte[] key_byte_set = SafeEncoder.encode(key);
                byte[] value_byte_set = SafeEncoder.encode(String.valueOf(value.getValue()));
                pipeline.sadd(key_byte_set, value_byte_set);
                break;
            case "zset":
                RedisKV.RedisKVValue score = kv.getScore();
                double score_value;
                try{
                    score_value = Double.valueOf(String.valueOf(score.getValue()));
                } catch(NumberFormatException ex){
                    throw new RuntimeException("redis score column parse long error:" + ex.toString());
                }
                byte[] key_byte_zset = SafeEncoder.encode(key);
                byte[] value_byte_zset = SafeEncoder.encode(String.valueOf(value.getValue()));
                pipeline.zadd(key_byte_zset, score_value, value_byte_zset);
                break;
            default:
                throw new RuntimeException(String.format("The type [%s] is not supported by redis.", value.getDataType().toString()));
        }
    }
}