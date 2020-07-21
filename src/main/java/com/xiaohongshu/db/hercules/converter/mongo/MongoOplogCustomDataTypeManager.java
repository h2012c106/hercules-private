package com.xiaohongshu.db.hercules.converter.mongo;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import org.json.JSONObject;

import java.util.List;

public class MongoOplogCustomDataTypeManager extends BaseCustomDataTypeManager<JSONObject, JSONObject> {

    public static final MongoOplogCustomDataTypeManager INSTANCE = new MongoOplogCustomDataTypeManager();

    @Override
    protected List<Class<? extends CustomDataType<JSONObject, JSONObject>>> generateTypeList() {
        return Lists.newArrayList(ObjectIdCustomDataType.class);
    }
}
