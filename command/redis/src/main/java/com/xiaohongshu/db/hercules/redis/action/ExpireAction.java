package com.xiaohongshu.db.hercules.redis.action;

import com.xiaohongshu.db.hercules.redis.RedisKV;
import redis.clients.jedis.Pipeline;

/**
 * Created by jamesqq on 2020/9/19.
 */
public class ExpireAction implements WriteAction{

    public void act(Pipeline pipeline, RedisKV kv, Integer expire) {
        pipeline.expire(kv.getKey().getValue().toString(), expire);
    }
}
