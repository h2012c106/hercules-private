package com.xiaohongshu.db.hercules.elasticsearchv7.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;

import java.util.List;
import java.util.Map;

public class ElasticsearchSchemaFetcher extends BaseSchemaFetcher {

    public ElasticsearchSchemaFetcher(GenericOptions options) {
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
