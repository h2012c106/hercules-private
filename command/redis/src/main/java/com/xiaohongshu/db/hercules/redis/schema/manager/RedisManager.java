package com.xiaohongshu.db.hercules.redis.schema.manager;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import com.xiaohongshu.db.hercules.redis.option.RedisOptionConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;


public class RedisManager {

    private static final Log log = LogFactory.getLog(RedisManager.class);

    private final GenericOptions options;
    private JedisPool jedisPool;
    private Jedis jedis;
    private DataType keyType;
    private DataType valueType;

    public RedisManager(GenericOptions options) {
        this.options = options;
        this.jedisPool = getJedisPool();
    }

    public JedisPool getJedisPool() {
        if (jedisPool == null) {
            synchronized (RedisManager.class) {
                if (jedisPool == null) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
//                    poolConfig.setMaxTotal(10);//资源池中的最大连接数
//                    poolConfig.setMaxIdle(10);//资源池允许的最大空闲连接数
//                    poolConfig.setMinIdle(2);//资源池确保的最少空闲连接数
//                    poolConfig.setMaxWaitMillis(30*1000);//当资源池连接用尽后，调用者的最大等待时间（单位为毫秒）。
//                    poolConfig.setTestOnBorrow(true);//向资源池借用连接时是否做连接有效性检测（ping）
//                    poolConfig.setTestOnReturn(true);//向资源池归还连接时是否做连接有效性检测（ping）
//                    poolConfig.setTimeBetweenEvictionRunsMillis(10*1000);//空闲资源的检测周期（单位为毫秒）
//                    poolConfig.setMinEvictableIdleTimeMillis(30*1000);//资源池中资源的最小空闲时间（单位为毫秒）
//                    poolConfig.setNumTestsPerEvictionRun(-1);//做空闲资源检测时，每次检测资源的个数，-1表示对所有连接做空闲监测
                    return new JedisPool(poolConfig, options.getString(RedisOptionConf.REDIS_HOST, ""), Integer.valueOf(options.getString(RedisOptionConf.REDIS_PORT, "12345")));
                }
            }
        }
        return jedisPool;
    }

    public void close() {
    }

    public void set(RedisKV kv) {
        if (jedis == null) {
            jedis = jedisPool.getResource();
        }

        String key = String.valueOf(kv.getKey().getValue());
        RedisKV.RedisKVValue value = kv.getValue();
        switch ((BaseDataType) value.getDataType()) {
            case STRING:
                String string_value = String.valueOf(value.getValue());
                jedis.set(key, string_value);
            case LIST:
                throw new RuntimeException("The type List is not supported by redis yet.");
            case MAP:
                Map<String, String> map_value = obj2Map(value.getValue());
                jedis.hmset(key, map_value);
            default:
                throw new RuntimeException(String.format("The type [%s] is not supported by redis.", value.getDataType().toString()));
        }
    }


    private static Map<String, String> obj2Map(Object obj) {
        Map<String, String> map = new HashMap<String, String>();
        // System.out.println(obj.getClass());
        // 获取f对象对应类中的所有属性域
        Field[] fields = obj.getClass().getDeclaredFields();
        for (int i = 0, len = fields.length; i < len; i++) {
            String varName = fields[i].getName();
            varName = varName.toLowerCase();//将key置为小写，默认为对象的属性
            try {
                // 获取原来的访问控制权限
                boolean accessFlag = fields[i].isAccessible();
                // 修改访问控制权限
                fields[i].setAccessible(true);
                // 获取在对象f中属性fields[i]对应的对象中的变量
                Object o = fields[i].get(obj);
                if (o != null)
                    map.put(varName, o.toString());
                // 恢复访问控制权限
                fields[i].setAccessible(accessFlag);
            } catch (IllegalArgumentException ex) {
                log.error("convert redis object to map occur illegaArgumentException:", ex);
            } catch (IllegalAccessException ex) {
                log.error("convert redis object to map occur illegaAccessException:", ex);
            }
        }
        return map;
    }

}