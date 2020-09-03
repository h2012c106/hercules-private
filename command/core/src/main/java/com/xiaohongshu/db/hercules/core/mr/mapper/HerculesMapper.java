package com.xiaohongshu.db.hercules.core.mr.mapper;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.xiaohongshu.db.hercules.core.filter.expr.Expr;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.LogUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Filter;
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
    public static final String ESTIMATED_WRITE_BYTE_SIZE_COUNTER_NAME = "Estimated write byte size";
    public static final String READ_RECORDS_COUNTER_NAME = "Read records num";
    public static final String WRITE_RECORDS_COUNTER_NAME = "Write records num";
    public static final String FILTERED_RECORDS_COUNTER_NAME = "Filtered records num";

    private static final Log LOG = LogFactory.getLog(HerculesMapper.class);

    private long rowProcessTime = 0L;
    private long filterTime = 0L;

    private Map<String, String> columnMap;
    private List<String> blackColumnList;

    private long mappedRecordNum = 0L;

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    @Filter
    private Expr filter;

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
        ++mappedRecordNum;

        // 读入行数++，因为可能存在把多行包进一行的序列化结构，故有DER的时候本值可能会偏大
        context.getCounter(HERCULES_GROUP_NAME, READ_RECORDS_COUNTER_NAME).increment(1L);

        // 如果上游读出来个null，无视这一行
        if (value == null) {
            return;
        }
        long start;

        start = System.currentTimeMillis();
        // 有filter，且本行filter结果为false，本行不写下去
        if (filter != null && !filter.getResult(value).asBoolean()) {
            context.getCounter(HERCULES_GROUP_NAME, FILTERED_RECORDS_COUNTER_NAME).increment(1L);
            return;
        }
        filterTime += (System.currentTimeMillis() - start);

        context.getCounter(HERCULES_GROUP_NAME, WRITE_RECORDS_COUNTER_NAME).increment(1L);
        context.getCounter(HERCULES_GROUP_NAME, ESTIMATED_WRITE_BYTE_SIZE_COUNTER_NAME).increment(value.getByteSize());

        start = System.currentTimeMillis();
        value = rowTransfer(value);
        rowProcessTime += (System.currentTimeMillis() - start);
        context.write(key, value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        super.cleanup(context);
        long cleanTime = (System.currentTimeMillis() - start);
        LOG.info(String.format("Map %s transferred %d record(s) using %.3fs for filter, %.3fs for row process and %.3fs for cleanup.",
                context.getTaskAttemptID().getTaskID().toString(),
                mappedRecordNum,
                (double) filterTime / 1000.0,
                (double) rowProcessTime / 1000.0,
                (double) cleanTime / 1000.0));
    }
}
