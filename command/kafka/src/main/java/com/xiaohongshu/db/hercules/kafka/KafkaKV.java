package com.xiaohongshu.db.hercules.kafka;

import com.xiaohongshu.db.hercules.core.datatype.DataType;

import static com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat.KEY_SEQ;
import static com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat.VALUE_SEQ;

public class KafkaKV {

    private KafkaKVValue key = null;
    private KafkaKVValue value = null;

    public KafkaKVValue getKey() {
        return key;
    }

    public void setKey(KafkaKVValue key) {
        this.key = key;
    }

    public KafkaKVValue getValue() {
        return value;
    }

    public void setValue(KafkaKVValue value) {
        this.value = value;
    }

    public void set(KafkaKVValue value, int columnSeq) {
        // 这里和writer约定好，用columnSeq来区分key/value
        if (columnSeq == KEY_SEQ) {
            setKey(value);
        } else if (columnSeq == VALUE_SEQ) {
            setValue(value);
        }
    }

    public static class KafkaKVValue {
        private DataType dataType;
        private Object value;
        private boolean isNull;

        public static KafkaKVValue initialize(DataType dataType, Object value) {
            KafkaKVValue res = new KafkaKVValue();
            res.dataType = dataType;
            res.value = value;
            res.isNull = value == null;
            return res;
        }

        public DataType getDataType() {
            return dataType;
        }

        public Object getValue() {
            return value;
        }

        public boolean isNull() {
            return isNull;
        }

        @Override
        public String toString() {
            return "KafkaKVValue{" +
                    "dataType=" + dataType +
                    ", value=" + value +
                    ", isNull=" + isNull +
                    '}';
        }
    }
}
