package com.xiaohongshu.db.hercules.core.mr.mapper;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.xiaohongshu.db.hercules.core.filter.expr.Expr;
import com.xiaohongshu.db.hercules.core.mr.udf.HerculesUDF;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.LogUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Filter;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesCounter;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf.MAP_STATUS_LOG_INTERVAL;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf.UDF;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseInputOptionsConf.BLACK_COLUMN;

public class HerculesMapper extends AutoProgressMapper<NullWritable, HerculesWritable, NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesMapper.class);

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

    private final List<HerculesUDF> udfList = new LinkedList<>();

    private final ScheduledExecutorService timingLoggerService = Executors.newSingleThreadScheduledExecutor();

    public HerculesMapper() {
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

        // 反射出UDF
        if (commonOptions.hasProperty(UDF)) {
            for (String className : commonOptions.getTrimmedStringArray(UDF, new String[0])) {
                HerculesUDF tmpUDF = HerculesContext.instance().getReflector().constructWithNonArgsConstructor(className, HerculesUDF.class);
                HerculesContext.instance().inject(tmpUDF);
                tmpUDF.initialize(context);
                udfList.add(tmpUDF);
            }
        }

        Long logInterval = commonOptions.getLong(MAP_STATUS_LOG_INTERVAL, null);
        if (logInterval > 0) {
            final NumberFormat numberFormat=NumberFormat.getPercentInstance();
            numberFormat.setMinimumFractionDigits(2);
            numberFormat.setRoundingMode(RoundingMode.HALF_UP);
            timingLoggerService.scheduleAtFixedRate(
                    new Runnable() {
                        @Override
                        public void run() {
                            LOG.info(String.format("Status <%s> / Progress <%s> / Counters: %s.",
                                    HerculesStatus.getHerculesMapStatus(),
                                    numberFormat.format(context.getProgress()),
                                    HerculesStatus.getStrValues()));
                        }
                    },
                    0,
                    logInterval,
                    TimeUnit.SECONDS
            );
        }
    }

    private HerculesWritable rowTransfer(HerculesWritable value) {
        // 黑名单处理
        WritableUtils.filterColumn(value.getRow(), blackColumnList);

        // 转换列名
        WritableUtils.convertColumnName(value, columnMap);

        return value;
    }

    @Override
    protected void map(NullWritable key, HerculesWritable value, Context context)
            throws IOException, InterruptedException {
        HerculesStatus.add(context, HerculesCounter.ESTIMATED_MAPPER_READ_BYTE_SIZE, value.getByteSize());

        ++mappedRecordNum;
        long start;
        HerculesStatus.setHerculesMapStatus(HerculesStatus.HerculesMapStatus.FILTERING);
        start = System.currentTimeMillis();
        // 有filter，且本行filter结果为false，本行不写下去
        if (filter != null && !filter.getResult(value).asBoolean()) {
            HerculesStatus.increase(context, HerculesCounter.FILTERED_RECORDS);
            return;
        }
        HerculesStatus.add(context, HerculesCounter.FILTER_TIME, System.currentTimeMillis() - start);

        HerculesStatus.setHerculesMapStatus(HerculesStatus.HerculesMapStatus.UDF);
        start = System.currentTimeMillis();
        for (HerculesUDF udf : udfList) {
            // 若udf返回null，这行不写
            if ((value = udf.evaluate(value)) == null) {
                HerculesStatus.increase(context, HerculesCounter.UDF_IGNORE_RECORDS);
                return;
            }
        }
        HerculesStatus.add(context, HerculesCounter.UDF_TIME, System.currentTimeMillis() - start);

        HerculesStatus.setHerculesMapStatus(HerculesStatus.HerculesMapStatus.MAPPING);
        start = System.currentTimeMillis();
        value = rowTransfer(value);
        HerculesStatus.add(context, HerculesCounter.ROW_PROCESS_TIME, System.currentTimeMillis() - start);
        context.write(key, value);

        HerculesStatus.add(context, HerculesCounter.ESTIMATED_MAPPER_WRITE_BYTE_SIZE, value.getByteSize());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (HerculesUDF udf : udfList) {
            udf.close();
        }
        timingLoggerService.shutdownNow();
        super.cleanup(context);
        LOG.info(String.format("Map %s transferred %d record(s) using %s for filter, %s for udf and %s for row process.",
                context.getTaskAttemptID().getTaskID().toString(),
                mappedRecordNum,
                HerculesStatus.getStrValue(HerculesCounter.FILTER_TIME),
                HerculesStatus.getStrValue(HerculesCounter.UDF_TIME),
                HerculesStatus.getStrValue(HerculesCounter.ROW_PROCESS_TIME)));
    }
}
