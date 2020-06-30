package com.xiaohongshu.db.hercules.core.utils;

import java.util.HashMap;
import java.util.Map;

public class StingyMap<K, V> extends HashMap<K, V> {

    public StingyMap(Map<? extends K, ? extends V> m) {
        super(m);
    }

    @Override
    public V get(Object key) {
        if (!super.containsKey(key)) {
            throw new RuntimeException("Unstored key: " + key);
        }
        return super.get(key);
    }
}
