package com.xiaohongshu.db.hercules.core.mr.udf;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 允许使用注解注入
 */
public abstract class HerculesUDF {

    abstract public void initialize(Mapper.Context context) throws IOException, InterruptedException;

    abstract public HerculesWritable evaluate(HerculesWritable row) throws IOException, InterruptedException;

    abstract public void close() throws IOException, InterruptedException;

}
