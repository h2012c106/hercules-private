package com.xiaohongshu.db.hercules.core.serialize;

import java.util.HashMap;

public class StingyMap<K, V> extends HashMap<K, V> {
    @Override
    public V get(Object key) {
        if (containsKey(key)) {
            return super.get(key);
        } else {
            throw new RuntimeException("You are ask for a unexisted key: " + key.toString());
        }
    }
}
