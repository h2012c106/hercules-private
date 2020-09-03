package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MongoDBWrapperGetterManager extends WrapperGetterFactory<Document> {

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private DataTypeConverter<Object, Document> converter;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

    private final Document fakeDocument = new Document(WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, 0);

    public MapWrapper documentToMapWrapper(Document document, String documentPosition)
            throws Exception {
        MapWrapper res = new MapWrapper(document.size());
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            String columnName = entry.getKey();
            // 用于columnTypeMap
            String fullColumnName = WritableUtils.concatColumn(documentPosition, columnName);
            Object columnValue = entry.getValue();
            DataType columnType = schema.getColumnTypeMap()
                    .getOrDefault(fullColumnName, converter.convertElementType(columnValue));
            res.put(columnName, getWrapperGetter(columnType).get(document, documentPosition, columnName, -1));
        }
        return res;
    }

    private ListWrapper listToListWrapper(List list) throws Exception {
        ListWrapper res = new ListWrapper(list.size());
        for (Object columnValue : list) {
            DataType columnType = converter.convertElementType(columnValue);
            fakeDocument.put(WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, columnValue);
            res.add(getWrapperGetter(columnType).get(fakeDocument, WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST, WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, -1));
        }
        return res;
    }

    @Override
    protected BaseTypeWrapperGetter.ByteGetter<Document> getByteGetter() {
        return new BaseTypeWrapperGetter.ByteGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName) == null;
            }

            @Override
            protected Byte getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Byte.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.ShortGetter<Document> getShortGetter() {
        return new BaseTypeWrapperGetter.ShortGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected Short getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Short.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.IntegerGetter<Document> getIntegerGetter() {
        return new BaseTypeWrapperGetter.IntegerGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected Integer getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Integer.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LongGetter<Document> getLongGetter() {
        return new BaseTypeWrapperGetter.LongGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected Long getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Long.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LonglongGetter<Document> getLonglongGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.FloatGetter<Document> getFloatGetter() {
        return new BaseTypeWrapperGetter.FloatGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected Float getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Float.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DoubleGetter<Document> getDoubleGetter() {
        return new BaseTypeWrapperGetter.DoubleGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected Double getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Double.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DecimalGetter<Document> getDecimalGetter() {
        return new BaseTypeWrapperGetter.DecimalGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected BigDecimal getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Decimal128.class).bigDecimalValue();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BooleanGetter<Document> getBooleanGetter() {
        return new BaseTypeWrapperGetter.BooleanGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected Boolean getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Boolean.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.StringGetter<Document> getStringGetter() {
        return new BaseTypeWrapperGetter.StringGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected String getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, String.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DateGetter<Document> getDateGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.TimeGetter<Document> getTimeGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.DatetimeGetter<Document> getDatetimeGetter() {
        return new BaseTypeWrapperGetter.DatetimeGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected ExtendedDate getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize(row.get(columnName, Date.class));
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BytesGetter<Document> getBytesGetter() {
        return new BaseTypeWrapperGetter.BytesGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return false;
            }

            @Override
            protected byte[] getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, Binary.class).getData();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.NullGetter<Document> getNullGetter() {
        return new BaseTypeWrapperGetter.NullGetter<Document>() {
            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return true;
            }

            @Override
            protected Void getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return null;
            }
        };
    }

    @Override
    protected WrapperGetter<Document> getListGetter() {
        return new WrapperGetter<Document>() {
            @Override
            protected DataType getType() {
                return BaseDataType.LIST;
            }

            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName) == null;
            }

            @Override
            protected BaseWrapper<?> getNonnull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return listToListWrapper(row.get(columnName, ArrayList.class));
            }
        };
    }

    @Override
    protected WrapperGetter<Document> getMapGetter() {
        return new WrapperGetter<Document>() {
            @Override
            protected DataType getType() {
                return BaseDataType.MAP;
            }

            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName) == null;
            }

            @Override
            protected BaseWrapper<?> getNonnull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                return documentToMapWrapper(row.get(columnName, Document.class), fullColumnName);
            }
        };
    }
}
