package com.xiaohongshu.db.hercules.mongodb.datatype;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BytesWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DateWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.StringWrapper;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.function.Function;

public class ObjectIdCustomDataType extends CustomDataType<Document, Document, ObjectId> {

    private static final String NAME = "OBJECTID";
    private static final BaseDataType BASE_DATA_TYPE = BaseDataType.STRING;
    private static final Class<?> STORAGE_CLASS = BASE_DATA_TYPE.getStorageClass();

    public static final ObjectIdCustomDataType INSTANCE = new ObjectIdCustomDataType();

    public ObjectIdCustomDataType() {
        super(NAME, BASE_DATA_TYPE, STORAGE_CLASS, new Function<Object, BaseWrapper<?>>() {
            @Override
            public BaseWrapper<?> apply(Object o) {
                return ObjectIdWrapper.get((ObjectId) o);
            }
        });
    }

    @Override
    protected BaseTypeWrapperGetter<ObjectId, Document> createWrapperGetter(final CustomDataType<Document, Document, ObjectId> self) {
        return new BaseTypeWrapperGetter<ObjectId, Document>() {
            @Override
            protected DataType getType() {
                return self;
            }

            @Override
            protected boolean isNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName) == null;
            }

            @Override
            protected ObjectId getNonnullValue(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.get(columnName, ObjectId.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter<ObjectId, Document> createWrapperSetter(final CustomDataType<Document, Document, ObjectId> self) {
        return new BaseTypeWrapperSetter<ObjectId, Document>() {
            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }

            @Override
            protected DataType getType() {
                return self;
            }

            @Override
            protected void setNonnullValue(ObjectId value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, value);
            }
        };
    }

    @Override
    protected ObjectId innerWrite(StringWrapper wrapper) throws Exception {
        String res = wrapper.asString();
        if (ObjectId.isValid(res)) {
            return new ObjectId(res);
        } else {
            throw new IllegalArgumentException("Illegal object id form: " + res);
        }
    }

    @Override
    protected ObjectId innerWrite(DateWrapper wrapper) throws Exception {
        return new ObjectId(wrapper.asDate().getDate());
    }

    @Override
    protected ObjectId innerWrite(BytesWrapper wrapper) throws Exception {
        return new ObjectId(wrapper.asBytes());
    }

    @Override
    protected ObjectId innerSpecialWrite(BaseWrapper<?> wrapper) throws Exception {
        if (wrapper.getClass() == ObjectIdWrapper.class) {
            return (ObjectId) wrapper.asDefault();
        } else {
            return super.innerSpecialWrite(wrapper);
        }
    }

    @Override
    public Class<?> getJavaClass() {
        return ObjectId.class;
    }
}
