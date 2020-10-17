package com.xiaohongshu.db.hercules.redis.action;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Pipeline;

public class RedisWriteAction {

    private static final Log log = LogFactory.getLog(RedisWriteAction.class);

    private Pipeline pipeline;

    public RedisWriteAction(Pipeline pipeline){
        this.pipeline = pipeline;
    }



}
