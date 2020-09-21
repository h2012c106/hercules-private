package com.xiaohognshu.db.hercules.udf;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.mr.udf.HerculesUDF;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RowStatisticsUDF extends HerculesUDF {

    private StatisticMode mode;
    private long byteSizeLimit = Long.MAX_VALUE;
    private String keyName;
    private long defaultByteSizeLimit = 1024 * 1025 * 5;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    @Override
    public void initialize(Mapper.Context context) throws IOException, InterruptedException {
        mode = StatisticMode.valueOfIgnoreCase(context.getConfiguration().get("hercules.udf.statisticMode"));
        try {
            byteSizeLimit = Long.parseLong(context.getConfiguration().get("hercules.udf.byteSizeLimit"));
        } catch (NumberFormatException e) {
            byteSizeLimit = defaultByteSizeLimit;
        }
        keyName = context.getConfiguration().get("hercules.udf.keyName");
    }

    // key totalLen {col: len}
    public HerculesWritable evaluate(HerculesWritable row) throws IOException, InterruptedException {
        HerculesWritable output = new HerculesWritable();
        long byteSize = row.getByteSize();
        switch (mode) {
            case FILTER:
                if (byteSize < byteSizeLimit) {
                    return null;
                }
            case LEN:
                output.put("total", IntegerWrapper.get(byteSize));
                addLenInfo(row, output);
                break;
            default:
                throw new RuntimeException("mode not supported.");
        }
        output.put(keyName, row.get(keyName));
        return output;
    }

    public void addLenInfo(HerculesWritable row, HerculesWritable output) {
        row.entrySet().forEach(entry -> output.put(entry.getKey(), IntegerWrapper.get(entry.getValue().getByteSize())));
    }

    public void close() throws IOException, InterruptedException {

    }
}

enum StatisticMode {

    LEN,
    FILTER;

    public static StatisticMode valueOfIgnoreCase(String value) {
        for (StatisticMode statisticMode : StatisticMode.values()) {
            if (StringUtils.equalsIgnoreCase(statisticMode.name(), value)) {
                return statisticMode;
            }
        }
        throw new ParseException("Illegal statistic mode: " + value);
    }
}
