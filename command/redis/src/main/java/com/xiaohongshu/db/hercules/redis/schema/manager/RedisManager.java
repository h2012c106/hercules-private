package com.xiaohongshu.db.hercules.redis.schema.manager;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import com.xiaohongshu.db.hercules.redis.action.InsertAction;
import com.xiaohongshu.db.hercules.redis.action.WriteAction;
import com.xiaohongshu.db.hercules.redis.option.RedisOptionConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RedisManager {

    private static final Log log = LogFactory.getLog(RedisManager.class);

    private final GenericOptions options;
    private JedisPool jedisPool;
    private Jedis jedis;
    private Pipeline pipeline;
    private final String writeType;
    private long batchNum = 0L;
    private final static int timeout = 60000;//redis pool读取输入InputStream的超时时间,单位毫秒

    private final long pipeSize;
    private int expire;
    private final String packageName = "com.xiaohongshu.db.hercules.redis.action.";
    private Map<String, ReflectMethod> methodMap = new HashMap<>();
    private int maxCount;
    private final InsertAction insertAction;

    public RedisManager(GenericOptions options) {
        this.options = options;
        this.jedisPool = getJedisPool();
        if(jedis != null)
            jedis.close();
        this.jedis = jedisPool.getResource();
        log.warn(" jedis:" + jedis.toString());
        Thread t = Thread.currentThread();
        log.warn(" thread name:" + t.getName() + "thread id:" + t.getId());
        this.pipeline = jedis.pipelined();
        this.pipeSize = options.getLong(RedisOptionConf.REDIS_PIPE_SIZE, RedisOptionConf.DEFAULT_PIPE_SIZE);
        this.maxCount = options.getInteger(CommonOptionsConf.NUM_MAPPER, 200);
        this.insertAction = new InsertAction();
        this.writeType = options.getString(RedisOptionConf.REDIS_WRITE_TPYE, "string");
    }

    public JedisPool getJedisPool() {
        if (jedisPool == null) {
            synchronized (RedisManager.class) {
                if (jedisPool == null) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    log.warn(" jedis pool maxcount:" + maxCount);
                    poolConfig.setMaxIdle(maxCount+20);
                    poolConfig.setMaxTotal(maxCount+20);
                    poolConfig.setTestOnBorrow(true);//向资源池借用连接时是否做连接有效性检测（ping）
                    poolConfig.setTestOnReturn(true);//向资源池归还连接时是否做连接有效性检测（ping）
                    return new JedisPool(poolConfig, options.getString(RedisOptionConf.REDIS_HOST, ""), Integer.valueOf(options.getString(RedisOptionConf.REDIS_PORT, "12345")), timeout);
                }
            }
        }
        return jedisPool;
    }

    public void close() {
        if (pipeline != null) {
            pipeline.sync();
            pipeline.close();
        }
        if (jedis != null) {
            jedis.close();
        }
    }

    private String captureFirstName(String name){
        String name_lower = name.toLowerCase();
        char[] cs = name_lower.toCharArray();
        cs[0] -= 32;
        return String.valueOf(cs);
    }

    public void initMethodMap(List<String> strategyList){
        try{
            for(String strategy : strategyList) {
                if(strategy.matches("expire(.*)") || strategy.matches("Expire(.*)")) {
                    expire = Integer.valueOf(strategy.substring(6));
                    strategy = strategy.substring(0,6);
                }
                String className = packageName + captureFirstName(strategy) + "Action";
                Class obj = Class.forName(className);
                Method method = obj.getMethod("act", Pipeline.class, RedisKV.class, Integer.class, String.class);
                ReflectMethod rl = new ReflectMethod();
                rl.setMethod(method);
                rl.setObj(obj);
                methodMap.put(strategy, rl);
            }
            log.warn(" origin strategyList is:" + strategyList);
            log.warn(" methodMap is:" + methodMap);
        } catch (Exception e){
            log.error(" redis strategy reflect error:", e);
            throw new RuntimeException(e);
        }
    }

    private void singleAct(String strategy, RedisKV kv){
        ReflectMethod rl = methodMap.get(strategy);
        Class obj = rl.getObj();
        Method method = rl.getMethod();
        try {
            method.invoke(obj.newInstance(), pipeline, kv, expire, writeType);
        } catch (Exception e){
            log.error(" redis strategy invoke error:", e);
        }
    }

    public void act(RedisKV kv, List<String> strategyList){
        if (methodMap.size() == 0)
            initMethodMap(strategyList);
        for (int i = 0; i < strategyList.size(); i++) {
            String strategy = strategyList.get(i);
            if(strategy.matches("expire(.*)") || strategy.matches("Expire(.*)"))
                strategy = "expire";
            singleAct(strategy, kv);
        }
        if ((++batchNum) >= pipeSize) {
            pipeline.sync();
            batchNum = 1L;
        }
    }

    public void set(RedisKV kv) {
        this.insertAction.act(pipeline, kv, expire, writeType);
        if ((++batchNum) >= pipeSize) {
            pipeline.sync();
            batchNum = 1L;
        }
    }

}