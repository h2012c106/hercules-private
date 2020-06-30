package com.xiaohongshu.db.hercules.mongodb.datatype;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BytesWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.NullWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.StringWrapper;
import org.bson.Document;
import org.bson.types.ObjectId;

public class ObjectIdCustomDataType extends CustomDataType<Document, Document> {

    private static final String NAME = "OBJECTID";
    private static final BaseDataType BASE_DATA_TYPE = BaseDataType.STRING;
    private static final Class<?> STORAGE_CLASS = BASE_DATA_TYPE.getStorageClass();

    public static final ObjectIdCustomDataType INSTANCE = new ObjectIdCustomDataType();

    public ObjectIdCustomDataType() {
        super(NAME, BASE_DATA_TYPE, STORAGE_CLASS);
    }

    @Override
    protected BaseWrapper innerRead(Document row, String rowName, String columnName, int columnSeq) throws Exception {
        ObjectId value = row.get(columnName, ObjectId.class);
        return StringWrapper.get(value == null ? null : value.toString());
    }

    @Override
    protected void innerWrite(StringWrapper wrapper, Document row, String rowName, String columnName, int columnSeq) throws Exception {
        String res = wrapper.asString();
        if (ObjectId.isValid(res)) {
            row.put(columnName, new ObjectId(res));
        } else {
            throw new IllegalArgumentException("Illegal object id form: " + res);
        }
    }

    @Override
    protected void innerWrite(BytesWrapper wrapper, Document row, String rowName, String columnName, int columnSeq) throws Exception {
        row.put(columnName, new ObjectId(wrapper.asBytes()));
    }

    @Override
    protected void innerWrite(NullWrapper wrapper, Document row, String rowName, String columnName, int columnSeq) throws Exception {
        row.put(columnName, null);
    }
}
