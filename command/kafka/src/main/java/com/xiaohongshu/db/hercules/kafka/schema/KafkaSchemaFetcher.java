package com.xiaohongshu.db.hercules.kafka.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaSchemaFetcher extends BaseSchemaFetcher<KafkaDataTypeConverter> {

    public KafkaSchemaFetcher(GenericOptions options, KafkaDataTypeConverter converter) {
        super(options, converter);
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
