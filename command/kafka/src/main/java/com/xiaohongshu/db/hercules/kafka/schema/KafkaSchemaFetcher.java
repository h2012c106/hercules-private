package com.xiaohongshu.db.hercules.kafka.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;

import java.util.List;
import java.util.Map;

public class KafkaSchemaFetcher extends BaseSchemaFetcher {
    public KafkaSchemaFetcher(GenericOptions options) {
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
