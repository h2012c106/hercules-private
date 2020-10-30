package com.xiaohongshu.db.hercules.redis.action;

import com.xiaohongshu.db.hercules.redis.RedisKV;
import redis.clients.jedis.Pipeline;

/**
 * Created by jamesqq on 2020/9/19.
 */
public class DeleteAction implements WriteAction{

    public void act(Pipeline pipeline, RedisKV redisKV, Integer expire, String writeType){
        pipeline.del(redisKV.getKey().getValue().toString());
    }
}
