package com.xiaohongshu.db.hercules.mongodb.schema;

import com.mongodb.client.MongoCollection;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.mongodb.datatype.MongoDBCustomDataTypeManager;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoDBSchemaFetcher extends BaseSchemaFetcher<MongoDBDataTypeConverter> {

    public MongoDBSchemaFetcher(GenericOptions options) {
        super(options, null, MongoDBCustomDataTypeManager.INSTANCE);
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        return null;
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap(Set<String> columnNameSet) {
        return null;
    }

    public static boolean isIndex(MongoCollection<Document> collection, String columnName) {
        for (Document indexInfo : collection.listIndexes()) {
            Document keyName = indexInfo.get("key", Document.class);
            Iterator<String> keyIterator = keyName.keySet().iterator();
            // 只看第一个，后面的也没一级索引
            if (keyIterator.hasNext()) {
                String firstKeyName = keyIterator.next();
                if (StringUtils.equalsIgnoreCase(firstKeyName, columnName)) {
                    return true;
                }
            }
        }
        return false;
    }
}
