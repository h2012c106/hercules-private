package com.xiaohongshu.db.hercules.nebula;

import java.util.LinkedHashMap;
import java.util.Map;

public class WritingRow {
    private final Map<String, Long> keyMap;
    private final Map<String, String> valueMap;

    public WritingRow() {
        this.keyMap = new LinkedHashMap<>();
        this.valueMap = new LinkedHashMap<>();
    }

    public void putKey(String columnName, Long keyValue) {
        keyMap.put(columnName, keyValue);
    }

    public void putValue(String columnName, String columnValueStr) {
        valueMap.put(columnName, columnValueStr);
    }

    public Map<String, Long> getKeyMap() {
        return keyMap;
    }

    public Map<String, String> getValueMap() {
        return valueMap;
    }
}
