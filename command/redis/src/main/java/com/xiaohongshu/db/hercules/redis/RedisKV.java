package com.xiaohongshu.db.hercules.redis;


import com.xiaohongshu.db.hercules.core.datatype.DataType;

public class RedisKV {

    public static final int KEY_SEQ = 0;
    public static final int VALUE_SEQ = 1;
    public static final int SCORE_SEQ = 2;

    private RedisKVValue key;
    private RedisKVValue value;
    private RedisKVValue score;

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

    public RedisKVValue getScore() {
        return score;
    }

    public void setScore(RedisKVValue score) {
        this.score = score;
    }

    public void set(RedisKVValue value, int columnSeq) {
        // 这里和writer约定好，用columnSeq来区分key/value
        if (columnSeq == KEY_SEQ) {
            setKey(value);
        } else if (columnSeq == VALUE_SEQ) {
            setValue(value);
        } else if (columnSeq == SCORE_SEQ) {
            setScore(value);
        }
    }

    public static class RedisKVValue {
        private DataType dataType;
        private Object value;

        public DataType getDataType() {
            return dataType;
        }

        public Object getValue() {
            return value;
        }

        public static RedisKVValue initialize(DataType dataType, Object value) {
            RedisKVValue res = new RedisKVValue();
            res.dataType = dataType;
            res.value = value;
            return res;
        }
    }
}
