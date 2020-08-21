package com.xiaohongshu.db.hercules.redis;


import com.xiaohongshu.db.hercules.core.datatype.DataType;

public class RedisKV {

    public static final int KEY_SEQ = 0;
    public static final int VALUE_SEQ = 1;

    private RedisKVValue key;
    private RedisKVValue value;

    public RedisKVValue getKey() {
        return key;
    }

    public void setKey(RedisKVValue key) {
        this.key = key;
    }

    public RedisKVValue getValue() {
        return value;
    }

    public void setValue(RedisKVValue value) {
        this.value = value;
    }

    public void set(RedisKVValue value, int columnSeq) {
        // 这里和writer约定好，用columnSeq来区分key/value
        if (columnSeq == KEY_SEQ) {
            setKey(value);
        } else if (columnSeq == VALUE_SEQ) {
            setValue(value);
        }
    }

public static class RedisKVValue{
        private DataType dataType;
        private Object value;
        private boolean isNull;

        public DataType getDataType() {
            return dataType;
        }

        public Object getValue() {
            return value;
        }

        public boolean isNull() {
            return isNull;
        }

        public static RedisKVValue initialize(DataType dataType, Object value) {
            RedisKVValue res = new RedisKVValue();
            res.dataType = dataType;
            res.value = value;
            return res;
        }
    }
}
