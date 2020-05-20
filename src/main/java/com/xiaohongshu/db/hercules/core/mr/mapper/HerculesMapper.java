package com.xiaohongshu.db.hercules.core.mr.mapper;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.BaseInputOptionsConf.BLACK_COLUMN;

public class HerculesMapper extends AutoProgressMapper<NullWritable, HerculesWritable, NullWritable, HerculesWritable> {

    public static final String HERCULES_GROUP_NAME = "Hercules Counters";
    public static final String ESTIMATED_BYTE_SIZE_COUNTER_NAME = "Estimated byte size";

    private static final Log LOG = LogFactory.getLog(HerculesMapper.class);

    private long time = 0;

    private Map<String, String> columnMap;
    private List<String> blackColumnList;

    public HerculesMapper() {
    }

    private HerculesWritable rowTransfer(HerculesWritable value) {
        // 黑名单处理
        WritableUtils.filterColumn(value.getRow(), blackColumnList);

        // TODO 列聚合

        // 转换列名
        WritableUtils.convertColumnName(value, columnMap);

        return value;
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
        columnMap = options.getCommonOptions()
                .getJson(CommonOptionsConf.COLUMN_MAP, new JSONObject())
                .getInnerMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> (String) entry.getValue()));
        blackColumnList = Arrays.asList(options.getSourceOptions().getStringArray(BLACK_COLUMN, null));
    }

    @Override
    protected void map(NullWritable key, HerculesWritable value, Context context)
            throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        context.getCounter(HERCULES_GROUP_NAME, ESTIMATED_BYTE_SIZE_COUNTER_NAME).increment(value.getByteSize());
        value = rowTransfer(value);
        context.write(key, value);
        time += (System.currentTimeMillis() - start);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        super.cleanup(context);
        time += (System.currentTimeMillis() - start);
        LOG.info(String.format("Spent %.3fs on mapping.", (double) time / 1000.0));
    }
}
