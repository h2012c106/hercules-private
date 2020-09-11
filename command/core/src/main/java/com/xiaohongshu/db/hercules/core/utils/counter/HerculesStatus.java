package com.xiaohongshu.db.hercules.core.utils.counter;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class HerculesStatus {

    public static final String GROUP_NAME = "Hercules Counters";

    private static final Map<HerculesCounter, AtomicLong> metrics = new ConcurrentHashMap<>();

    public static void increase(TaskAttemptContext context, HerculesCounter counter) {
        add(context, counter, 1L);
    }

    public static void add(TaskAttemptContext context, HerculesCounter counter, long value) {
        metrics.computeIfAbsent(counter, key -> new AtomicLong(0L)).addAndGet(value);
        if (counter.isRecordToMRCounter()) {
            context.getCounter(GROUP_NAME, counter.getCounterName()).increment(value);
        }
    }

    public static long getValue(HerculesCounter counter) {
        return metrics.getOrDefault(counter, new AtomicLong(0L)).longValue();
    }

    public static Map<String, Long> getValues() {
        Map<String, Long> res = new TreeMap<>();
        for (Map.Entry<HerculesCounter, AtomicLong> entry : metrics.entrySet()) {
            res.put(entry.getKey().getCounterName(), entry.getValue().longValue());
        }
        return res;
    }

    public static String getStrValue(HerculesCounter counter) {
        return counter.getToStringFunc().apply(metrics.getOrDefault(counter, new AtomicLong(0L)).longValue());
    }

    public static Map<String, String> getStrValues() {
        Map<String, String> res = new TreeMap<>();
        for (Map.Entry<HerculesCounter, AtomicLong> entry : metrics.entrySet()) {
            res.put(entry.getKey().getCounterName(), entry.getKey().getToStringFunc().apply(entry.getValue().longValue()));
        }
        return res;
    }

    private volatile static HerculesMapStatus herculesMapStatus = HerculesMapStatus.INITIALIZE;

    public static HerculesMapStatus getHerculesMapStatus() {
        return herculesMapStatus;
    }

    public static void setHerculesMapStatus(HerculesMapStatus herculesMapStatus) {
        HerculesStatus.herculesMapStatus = herculesMapStatus;
    }

    public enum HerculesMapStatus {
        INITIALIZE,
        READING,
        DESERIALIZING,
        UDF,
        FILTERING,
        MAPPING,
        SERIALIZING,
        WRITING
    }

}
