package com.xiaohongshu.db.hercules.core.utils.counter;

import java.util.function.Function;

public enum HerculesCounter {
    /**
     * 行数计数器
     */
    READ_RECORDS("Read records num", true, String::valueOf),
    WRITE_RECORDS("Write records num", true, String::valueOf),
    DER_RECORDS("Deserialize records num", true, String::valueOf),
    DER_IGNORE_RECORDS("Deserialize ignored records num (missing key or value)", true, String::valueOf),
    DER_ACTUAL_RECORDS("Deserialize actual records num", true, String::valueOf),
    FILTERED_RECORDS("Filtered records num", true, String::valueOf),
    UDF_IGNORE_RECORDS("UDF ignored records num", true, String::valueOf),
    SER_RECORDS("Serialize records num", true, String::valueOf),
    SER_ACTUAL_RECORDS("Serialize ignored records num (missing key or value)", true, String::valueOf),
    SER_IGNORE_RECORDS("Serialize actual records num", true, String::valueOf),
    /**
     * 空间计数器
     */
    ESTIMATED_MAPPER_READ_BYTE_SIZE("Estimated mapper read byte size", true, String::valueOf),
    ESTIMATED_MAPPER_WRITE_BYTE_SIZE("Estimated mapper write byte size", true, String::valueOf),
    /**
     * 计时器（map间累加无意义）
     */
    READ_TIME("Read time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    }),
    WRITE_TIME("Write time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    }),
    FILTER_TIME("Filter time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    }),
    UDF_TIME("UDF time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    }),
    ROW_PROCESS_TIME("Write time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    }),
    DER_TIME("Deserialize time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    }),
    SER_TIME("Serialize time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    }),
    QPS_CONTROL_WAITING_TIME("Write time (ms)", false, aLong -> {
        return String.format("%.3fs", aLong.doubleValue() / 1000.0);
    });

    private final String counterName;
    private final boolean recordToMRCounter;
    private final Function<Long, String> toStringFunc;

    HerculesCounter(String counterName, boolean recordToMRCounter, Function<Long, String> toStringFunc) {
        this.counterName = counterName;
        this.recordToMRCounter = recordToMRCounter;
        this.toStringFunc = toStringFunc;
    }

    public String getCounterName() {
        return counterName;
    }

    public boolean isRecordToMRCounter() {
        return recordToMRCounter;
    }

    public Function<Long, String> getToStringFunc() {
        return toStringFunc;
    }
}