package com.xiaohongshu.db.hercules.kafka.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaSchemaFetcher extends BaseSchemaFetcher {

    public KafkaSchemaFetcher(GenericOptions options, KafkaDataTypeConverter converter) {
        super(options, converter);
    }

    @Override
    protected List<String> getColumnNameList() {
        return null;
    }

    protected Map<String, DataType> getColumnTypeMap(Set<String> columnNameSet) {
        return null;
    }
}
