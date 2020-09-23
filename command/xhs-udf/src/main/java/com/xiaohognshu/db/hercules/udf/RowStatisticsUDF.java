package com.xiaohognshu.db.hercules.udf;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.mr.udf.HerculesUDF;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.StringWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class RowStatisticsUDF extends HerculesUDF {

    private final String HERCULES_UDF_STATISTIC_MODE = "hercules.udf.statisticMode";
    private final String HERCULES_UDF_TOTAL_BYTESIZ_ELIMIT = "hercules.udf.totalByteSizeLimit";
    private final String HERCULES_UDF_COLUMN_BYTESIZ_ELIMIT = "hercules.udf.columnByteSizeLimit";
    private final String HERCULES_UDF_KEY_NAME = "hercules.udf.keyName";

    private StatisticMode mode;
    private String keyName;
    private long totalByteSizeLimit = Long.MAX_VALUE;
    private long defaultTotalByteSizeLimit = 1024 * 1024 * 100;

    private long columnByteSizeLimit = Long.MAX_VALUE;
    private long defaultColumnByteSizeLimit = 1024 * 1024 * 100;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    @Override
    public void initialize(Mapper.Context context) throws IOException, InterruptedException {
        mode = StatisticMode.valueOfIgnoreCase(context.getConfiguration().get(HERCULES_UDF_STATISTIC_MODE));
        try {
            totalByteSizeLimit = Long.parseLong(context.getConfiguration().get(HERCULES_UDF_TOTAL_BYTESIZ_ELIMIT));
        } catch (NumberFormatException e) {
            totalByteSizeLimit = defaultTotalByteSizeLimit;
        }
        try {
            columnByteSizeLimit = Long.parseLong(context.getConfiguration().get(HERCULES_UDF_COLUMN_BYTESIZ_ELIMIT));
        } catch (NumberFormatException e) {
            columnByteSizeLimit = defaultColumnByteSizeLimit;
        }
        keyName = context.getConfiguration().get(HERCULES_UDF_KEY_NAME);
        if (keyName == null) {
            throw new RuntimeException("keyName should be provided with -D" + HERCULES_UDF_KEY_NAME);
        }
    }

    // key totalLen {col: len}
    public HerculesWritable evaluate(HerculesWritable row) throws IOException, InterruptedException {
        HerculesWritable output = new HerculesWritable();
        long byteSize = row.getByteSize();
        switch (mode) {
            case FILTER:
                if (byteSize < totalByteSizeLimit) {
                    return null;
                }
            case LEN:
                output.put("TOTAL_BYTES_COUNT", IntegerWrapper.get(byteSize));
                addLenInfo(row, output);
                break;
            default:
                throw new RuntimeException("mode not supported.");
        }
        try {
            output.put(keyName, StringWrapper.get(row.get(keyName).asString()));
        } catch (Throwable e) {
            throw new RuntimeException(row.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()).toString());
        }
        return output;
    }

    public void addLenInfo(HerculesWritable row, HerculesWritable output) {
        row.entrySet().forEach(entry -> {
            switch (mode) {
                case FILTER:
                    if (entry.getValue().getByteSize() < columnByteSizeLimit) {
                        return;
                    }
                case LEN:
                    output.put(entry.getKey(), IntegerWrapper.get(entry.getValue().getByteSize()));
                    break;
                default:
                    throw new RuntimeException("mode not supported.");
            }
        });
    }

    @Override
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
