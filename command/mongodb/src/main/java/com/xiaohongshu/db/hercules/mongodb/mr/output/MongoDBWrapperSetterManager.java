package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import lombok.NonNull;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.util.ArrayList;

class MongoDBWrapperSetterManager extends WrapperSetterFactory<Document> {

    private boolean decimalAsString;

    public MongoDBWrapperSetterManager(Schema schema) {
        super(schema);
    }

    public void setDecimalAsString(boolean decimalAsString) {
        this.decimalAsString = decimalAsString;
    }

    /**
     * 当上游不是list时，做一个singleton list
     *
     * @param wrapper
     * @return
     * @throws Exception
     */
    private ArrayList<Object> wrapperToList(BaseWrapper<?> wrapper) throws Exception {
        ArrayList<Object> res;
        if (wrapper.getType() == BaseDataType.LIST) {
            ListWrapper listWrapper = (ListWrapper) wrapper;
            res = new ArrayList<>(listWrapper.size());
            for (int i = 0; i < listWrapper.size(); ++i) {
                BaseWrapper<?> subWrapper = listWrapper.get(i);
                // 由于不能指定list下的类型故只能抄作业
                DataType dataType = subWrapper.getType();
                // 不能像reader一样共享一个，写会有多线程情况，要慎重
                Document tmpDocument = new Document();
                getWrapperSetter(dataType).set(subWrapper, tmpDocument, WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST, WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, -1);
                Object convertedValue = tmpDocument.get(WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST);
                res.add(convertedValue);
            }
        } else {
            DataType dataType = wrapper.getType();
            Document tmpDocument = new Document();
            getWrapperSetter(dataType).set(wrapper, tmpDocument, WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST, WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, -1);
            Object convertedValue = tmpDocument.get(WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST);
            res = Lists.newArrayList(convertedValue);
        }
        return res;
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<Document> getByteSetter() {
        return new BaseTypeWrapperSetter.ByteSetter<Document>() {
            @Override
            protected void setNonnullValue(Byte value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<Document> getShortSetter() {
        return new BaseTypeWrapperSetter.ShortSetter<Document>() {
            @Override
            protected void setNonnullValue(Short value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<Document> getIntegerSetter() {
        return new BaseTypeWrapperSetter.IntegerSetter<Document>() {
            @Override
            protected void setNonnullValue(Integer value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<Document> getLongSetter() {
        return new BaseTypeWrapperSetter.LongSetter<Document>() {
            @Override
            protected void setNonnullValue(Long value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<Document> getLonglongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<Document> getFloatSetter() {
        return new BaseTypeWrapperSetter.FloatSetter<Document>() {
            @Override
            protected void setNonnullValue(Float value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<Document> getDoubleSetter() {
        return new BaseTypeWrapperSetter.DoubleSetter<Document>() {
            @Override
            protected void setNonnullValue(Double value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<Document> getDecimalSetter() {
        return new BaseTypeWrapperSetter.DecimalSetter<Document>() {
            @Override
            protected void setNonnullValue(BigDecimal value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                if (decimalAsString) {
                    row.put(columnName, value.toPlainString());
                } else {
                    row.put(columnName, new Decimal128(value));
                }
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<Document> getBooleanSetter() {
        return new BaseTypeWrapperSetter.BooleanSetter<Document>() {
            @Override
            protected void setNonnullValue(Boolean value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<Document> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<Document>() {
            @Override
            protected void setNonnullValue(String value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<Document> getDateSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<Document> getTimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<Document> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<Document>() {
            @Override
            protected void setNonnullValue(ExtendedDate value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value.getDate());
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<Document> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<Document>() {
            @Override
            protected void setNonnullValue(byte[] value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, new Binary(value));
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<Document> getNullSetter() {
        return new BaseTypeWrapperSetter.NullSetter<Document>() {
            @Override
            protected void setNonnullValue(Void value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected WrapperSetter<Document> getListSetter() {
        return new WrapperSetter<Document>() {
            @Override
            protected void setNonnull(@NonNull BaseWrapper<?> wrapper, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, wrapperToList(wrapper));
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected WrapperSetter<Document> getMapSetter() {
        return new WrapperSetter<Document>() {
            @Override
            protected void setNonnull(BaseWrapper<?> wrapper, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                if (wrapper.getType() == BaseDataType.MAP) {
                    // 确保尽可能不丢失类型信息
                    String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                    row.put(columnName, writeMapWrapper((MapWrapper) wrapper, new Document(), fullColumnName));
                } else {
                    row.put(columnName, ((JSONObject) wrapper.asJson()).getInnerMap());
                }
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }
}
