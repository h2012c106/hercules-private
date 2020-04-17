package com.xiaohongshu.db.hercules.core.mr.mapper;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HerculesMapper extends AutoProgressMapper<NullWritable, HerculesWritable, NullWritable, HerculesWritable> {
    public static final String HERCULES_GROUP_NAME = "Hercules Counters";
    public static final String ESTIMATED_BYTE_SIZE_COUNTER_NAME = "Estimated byte size";

    private static final Log LOG = LogFactory.getLog(HerculesMapper.class);

    public HerculesMapper() {
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());
        GenericOptions commonOptions = new GenericOptions();

        // 处理log-level
        Logger.getRootLogger().setLevel(
                Level.toLevel(
                        commonOptions.getString(
                                CommonOptionsConf.LOG_LEVEL, CommonOptionsConf.DEFAULT_LOG_LEVEL.toString()
                        )
                )
        );

        // 注册时间格式
        DateUtils.setFormats(options.getSourceOptions(), options.getTargetOptions());

        // 注册columnMap
        HerculesWritable.setColumnNameMap(options.getCommonOptions().getJson(CommonOptionsConf.COLUMN_MAP, new JSONObject()));
    }

    @Override
    protected void map(NullWritable key, HerculesWritable value, Context context)
            throws IOException, InterruptedException {
        context.getCounter(HERCULES_GROUP_NAME, ESTIMATED_BYTE_SIZE_COUNTER_NAME).increment(value.getByteSize());
        context.write(key, value);
    }
}
