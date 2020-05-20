package com.xiaohongshu.db.hercules.mongodb.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.DataType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoDBSchemaFetcher extends BaseSchemaFetcher<MongoDBDataTypeConverter> {

    public MongoDBSchemaFetcher(GenericOptions options) {
        super(options, null);
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        return null;
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap(Set<String> columnNameSet) {
        return null;
    }
}
