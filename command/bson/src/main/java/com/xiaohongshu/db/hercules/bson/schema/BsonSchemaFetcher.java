package com.xiaohongshu.db.hercules.bson.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;

import java.util.List;
import java.util.Map;

public class BsonSchemaFetcher extends BaseSchemaFetcher {

    public BsonSchemaFetcher(GenericOptions options) {
        super(options);
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        return null;
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap() {
        return null;
    }
}
