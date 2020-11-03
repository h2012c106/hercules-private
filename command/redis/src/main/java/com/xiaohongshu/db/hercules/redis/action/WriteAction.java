package com.xiaohongshu.db.hercules.redis.action;


import com.xiaohongshu.db.hercules.redis.RedisKV;
import redis.clients.jedis.Pipeline;

public interface WriteAction {

   void act(Pipeline pipeline, RedisKV redisKV, Integer expire, String writeType);

}
