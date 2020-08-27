package com.xiaohongshu.db.hercules.core.mr.mapper;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.core.utils.LogUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
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

import static com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseInputOptionsConf.BLACK_COLUMN;

public class HerculesMapper extends AutoProgressMapper<NullWritable, HerculesWritable, NullWritable, HerculesWritable> {

    public static final String HERCULES_GROUP_NAME = "Hercules Counters";
    public static final String ESTIMATED_BYTE_SIZE_COUNTER_NAME = "Estimated byte size";

    private static final Log LOG = LogFactory.getLog(HerculesMapper.class);

    private long time = 0;

    private Map<String, String> columnMap;
    private List<String> blackColumnList;

    private long readRecordNum = 0L;

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    public HerculesMapper() {
    }

    private HerculesWritable rowTransfer(HerculesWritable value) {
        // 黑名单处理
        WritableUtils.filterColumn(value.getRow(), blackColumnList);

        // 转换列名
        WritableUtils.convertColumnName(value, columnMap);

        return value;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // 初始化context，因为hadoop的这三兄弟的构造都不归我管，且仅在进入这几个函数时我能获得configuration用以初始化context，
        // 但是这几个函数初次调用顺序并不一定，所以都做一次，context内部仅会做一次初始化
        HerculesContext.initialize(context.getConfiguration()).inject(this);

        LogUtils.configureLog4J();

        // 处理log-level
        Logger.getRootLogger().setLevel(
                Level.toLevel(
                        commonOptions.getString(
                                CommonOptionsConf.LOG_LEVEL, CommonOptionsConf.DEFAULT_LOG_LEVEL.toString()
                        )
                )
        );

        // 注册时间格式
        DateUtils.setFormats(sourceOptions, targetOptions);

        // 注册columnMap
        columnMap = commonOptions
                .getJson(CommonOptionsConf.COLUMN_MAP, new JSONObject())
                .getInnerMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> (String) entry.getValue()));
        blackColumnList = Arrays.asList(sourceOptions.getTrimmedStringArray(BLACK_COLUMN, null));
    }

    @Override
    protected void map(NullWritable key, HerculesWritable value, Context context)
            throws IOException, InterruptedException {
        // 如果上游读出来个null，无视这一行
        if (value == null) {
            return;
        }
        long start = System.currentTimeMillis();
        context.getCounter(HERCULES_GROUP_NAME, ESTIMATED_BYTE_SIZE_COUNTER_NAME).increment(value.getByteSize());
        value = rowTransfer(value);
        time += (System.currentTimeMillis() - start);
        ++readRecordNum;
        context.write(key, value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        super.cleanup(context);
        time += (System.currentTimeMillis() - start);
        LOG.info(String.format("Map %s transferred %d record(s) using %.3fs.",
                context.getTaskAttemptID().getTaskID().toString(),
                readRecordNum,
                (double) time / 1000.0));
    }
}
