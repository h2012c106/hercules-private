package com.xiaohongshu.db.hercules.mongodb.datatype;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import org.bson.Document;

import java.util.List;

public class MongoDBCustomDataTypeManager extends BaseCustomDataTypeManager<Document, Document> {

    public static final MongoDBCustomDataTypeManager INSTANCE = new MongoDBCustomDataTypeManager();

    @Override
    protected List<Class<? extends CustomDataType<Document, Document>>> generateTypeList() {
        return Lists.newArrayList(ObjectIdCustomDataType.class);
    }
}
