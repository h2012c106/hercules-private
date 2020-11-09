package com.xiaohongshu.db.hercules.redis.action;

import com.xiaohongshu.db.hercules.redis.RedisKV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Pipeline;

/**
 * Created by jamesqq on 2020/9/19.
 */
public class DeleteAction implements WriteAction{

    private static final Log log = LogFactory.getLog(DeleteAction.class);

    public void act(Pipeline pipeline, RedisKV redisKV, Integer expire, String writeType){
        pipeline.del(String.valueOf(redisKV.getKey().getValue()));
    }
}
