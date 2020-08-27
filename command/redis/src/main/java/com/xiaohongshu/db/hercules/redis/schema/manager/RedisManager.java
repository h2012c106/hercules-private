package com.xiaohongshu.db.hercules.redis.schema.manager;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import com.xiaohongshu.db.hercules.redis.option.RedisOptionConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;


public class RedisManager {

    private static final Log log = LogFactory.getLog(RedisManager.class);

    private final GenericOptions options;
    private JedisPool jedisPool;
    private Jedis jedis;
    private Pipeline pipeline;
    private long batchNum;
    private final static int timeout = 60000;//redis pool读取输入InputStream的超时时间,单位毫秒

    public RedisManager(GenericOptions options) {
        this.options = options;
        this.jedisPool = getJedisPool();
        this.jedis = jedisPool.getResource();
        this.pipeline = jedis.pipelined();
    }

    public JedisPool getJedisPool() {
        if (jedisPool == null) {
            synchronized (RedisManager.class) {
                if (jedisPool == null) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setTestOnBorrow(true);//向资源池借用连接时是否做连接有效性检测（ping）
                    poolConfig.setTestOnReturn(true);//向资源池归还连接时是否做连接有效性检测（ping）
                    return new JedisPool(poolConfig, options.getString(RedisOptionConf.REDIS_HOST, ""), Integer.valueOf(options.getString(RedisOptionConf.REDIS_PORT, "12345")), timeout);
                }
            }
        }
        return jedisPool;
    }

    public void close() {
        if(pipeline != null){
            pipeline.sync();
            pipeline.close();
        }
        if(jedis != null){
            jedis.close();
        }
    }

    public void set(RedisKV kv, long pipe_size) {
        String key = String.valueOf(kv.getKey().getValue());
        RedisKV.RedisKVValue value = kv.getValue();
        if(key == null || value.getValue() == null){
            log.debug("the key or value is null: key:" + key + " value:" + String.valueOf(value.getValue()));
            return;
        }
        switch ((BaseDataType) value.getDataType()) {
            case STRING:
                String string_value = String.valueOf(value.getValue());
                pipeline.set(key, string_value);
                break;
            case LIST:
                throw new RuntimeException("The type List is not supported by redis yet.");
            case MAP:
                Map<String, String> map_value = (Map<String, String>)(value.getValue());
                pipeline.hmset(key, map_value);
                break;
            default:
                throw new RuntimeException(String.format("The type [%s] is not supported by redis.", value.getDataType().toString()));
        }
        if((++batchNum) >= pipe_size){
            pipeline.sync();
            batchNum = 0;
        }
    }

}