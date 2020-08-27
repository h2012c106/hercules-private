package com.xiaohongshu.db.hercules.parquetschema.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetSchemaNegotiatorContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetSchemaSchemaNegotiatorContext extends ParquetSchemaNegotiatorContext {

    public ParquetSchemaSchemaNegotiatorContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void afterAll(List<String> columnName, Map<String, DataType> columnType) {
        super.afterAll(columnName, columnType);
        if (!StringUtils.isEmpty(getOptions().getString(MESSAGE_TYPE, null))) {
            throw new RuntimeException("The parquet schema can already be calculated by source info, unnecessary to generate it, exit.");
        }
    }
}
