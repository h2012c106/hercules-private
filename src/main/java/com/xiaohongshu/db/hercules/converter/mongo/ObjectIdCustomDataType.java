package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.NullWrapper;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;

public class ObjectIdCustomDataType  extends CustomDataType<JSONObject, JSONObject> {


    private static final String NAME = "OBJECTID";
    private static final BaseDataType BASE_DATA_TYPE = BaseDataType.STRING;
    private static final Class<?> STORAGE_CLASS = BASE_DATA_TYPE.getStorageClass();

    public static final ObjectIdCustomDataType INSTANCE = new ObjectIdCustomDataType();

    public ObjectIdCustomDataType() {
        super(NAME, BASE_DATA_TYPE, STORAGE_CLASS);
    }

    @Override
    protected BaseWrapper innerRead(JSONObject row, String rowName, String columnName, int columnSeq) throws Exception {
        return null;
    }

    @Override
    protected void innerWrite(NullWrapper wrapper, JSONObject row, String rowName, String columnName, int columnSeq) throws Exception {
        String res = wrapper.asString();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("$oid", res);
        row.put(columnName, jsonObject);
    }
}
