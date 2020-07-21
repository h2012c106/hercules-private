package com.xiaohongshu.db.hercules.converter.mongo;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import lombok.NonNull;
import org.apache.commons.codec.binary.Hex;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST;
import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST;

public class MongoOplogWrapperSetterFactory extends WrapperSetterFactory<JSONObject> {

    protected Map<String, DataType> columnTypeMap;

    public void setColumnTypeMap(Map<String, DataType> columnTypeMap) {
        this.columnTypeMap = columnTypeMap;
    }

    @Override
    protected WrapperSetter<JSONObject> getByteSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null) {
                doc.put(name, null);
            } else {
                doc.put(name, res.byteValueExact());
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getShortSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null) {
                doc.put(name, null);
            } else {
                doc.put(name, res.shortValueExact());
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getIntegerSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null) {
                doc.put(name, null);
            } else {
                doc.put(name, res.intValueExact());
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getLongSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null) {
                doc.put(name, null);
            } else {
                doc.put(name, res.longValueExact());
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getLonglongSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<JSONObject> getFloatSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            BigDecimal res = wrapper.asBigDecimal();
            if (res == null) {
                doc.put(name, null);
            } else {
                doc.put(name, OverflowUtils.numberToFloat(res));
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getDoubleSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            BigDecimal res = wrapper.asBigDecimal();
            if (res == null) {
                doc.put(name, null);
            } else {
                doc.put(name, OverflowUtils.numberToDouble(res));
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getDecimalSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            BigDecimal res = wrapper.asBigDecimal();
            // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
            if (res == null) {
                doc.put(name, null);
            } else {
                doc.put(name, new Decimal128(res));
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getBooleanSetter() {
        return (wrapper, doc, cf, name, seq) -> doc.put(name, wrapper.asBoolean());
    }

    @Override
    protected WrapperSetter<JSONObject> getStringSetter() {
        return (wrapper, doc, cf, name, seq) -> doc.put(name, wrapper.asString());
    }

    @Override
    protected WrapperSetter<JSONObject> getDateSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<JSONObject> getTimeSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<JSONObject> getDatetimeSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("$date", wrapper.asDate().getTime());
            doc.put(name, jsonObject);
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getBytesSetter() {
//        return (wrapper, doc, cf, name, seq) -> doc.put(name, new String(wrapper.asBytes(), StandardCharsets.UTF_8)); // canal entry 实现
        return (wrapper, doc, cf, name, seq) -> doc.put(name, Hex.encodeHexString(wrapper.asBytes())); // hexString 实现
    }

    @Override
    protected WrapperSetter<JSONObject> getNullSetter() {
        return (wrapper, doc, cf, name, seq) -> doc.put(name, null);
    }


    @Override
    protected WrapperSetter<JSONObject> getListSetter() {
        return (wrapper, doc, cf, name, seq) -> {
            if (wrapper.isNull()) {
                doc.put(name, null);
            } else {
                doc.put(name, wrapperToList(wrapper));
            }
        };
    }

    @Override
    protected WrapperSetter<JSONObject> getMapSetter() {
        return new WrapperSetter<JSONObject>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, JSONObject row, String rowName, String columnName, int columnSeq)
                    throws Exception {
                if (wrapper.isNull()) {
                    // 有可能是个null，不处理else要NPE
                    row.put(columnName, null);
                } else if (wrapper.getType() == BaseDataType.MAP) {
                    // 确保尽可能不丢失类型信息
                    String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                    row.put(columnName, mapWrapperToDocument((MapWrapper) wrapper, fullColumnName));
                } else {
                    row.put(columnName, ((com.alibaba.fastjson.JSONObject) wrapper.asJson()).getInnerMap());
                }
            }
        };
    }

    private ArrayList<Object> wrapperToList(BaseWrapper wrapper) throws Exception {
        ArrayList<Object> res;
        if (wrapper.getType() == BaseDataType.LIST) {
            ListWrapper listWrapper = (ListWrapper) wrapper;
            res = new ArrayList<>(listWrapper.size());
            for (int i = 0; i < listWrapper.size(); ++i) {
                BaseWrapper subWrapper = listWrapper.get(i);
                // 由于不能指定list下的类型故只能抄作业
                DataType dataType = subWrapper.getType();
                // 不能像reader一样共享一个，写会有多线程情况，要慎重
                JSONObject jsonObject = new JSONObject();
                getWrapperSetter(dataType).set(subWrapper, jsonObject, FAKE_PARENT_NAME_USED_BY_LIST, FAKE_COLUMN_NAME_USED_BY_LIST, -1);
                Object convertedValue = jsonObject.get(FAKE_COLUMN_NAME_USED_BY_LIST);
                res.add(convertedValue);
            }
        } else {
            DataType dataType = wrapper.getType();
            JSONObject jsonObject = new JSONObject();
            getWrapperSetter(dataType).set(wrapper, jsonObject, FAKE_PARENT_NAME_USED_BY_LIST, FAKE_COLUMN_NAME_USED_BY_LIST, -1);
            Object convertedValue = jsonObject.get(FAKE_COLUMN_NAME_USED_BY_LIST);
            res = Lists.newArrayList(convertedValue);
        }
        return res;
    }

    private JSONObject mapWrapperToDocument(MapWrapper wrapper, String columnPath) throws Exception {
        JSONObject res = new JSONObject();
        for (Map.Entry<String, BaseWrapper> entry : wrapper.entrySet()) {
            String columnName = entry.getKey();
            String fullColumnName = WritableUtils.concatColumn(columnPath, columnName);
            BaseWrapper subWrapper = entry.getValue();
            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, subWrapper.getType());
            getWrapperSetter(columnType).set(subWrapper, res, columnPath, columnName, -1);
        }
        return res;
    }
}