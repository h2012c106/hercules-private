package com.xiaohongshu.db.hercules.core.utils.counter;

import java.util.function.Function;

public interface Counter {
    public String getCounterName();

    public boolean isRecordToMRCounter();

    public Function<Long, String> getToStringFunc();
}
