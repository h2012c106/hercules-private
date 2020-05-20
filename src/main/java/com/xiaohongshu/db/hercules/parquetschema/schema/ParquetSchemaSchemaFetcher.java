package com.xiaohongshu.db.hercules.parquetschema.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetSchemaFetcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetSchemaSchemaFetcher extends ParquetSchemaFetcher {

    private static final Log LOG = LogFactory.getLog(ParquetSchemaSchemaFetcher.class);

    public ParquetSchemaSchemaFetcher(GenericOptions options, ParquetDataTypeConverter converter) {
        super(options, converter);
    }

    @Override
    public void postNegotiate(List<String> columnNameList, Map<String, DataType> columnTypeMap) {
        super.postNegotiate(columnNameList, columnTypeMap);
        if (!StringUtils.isEmpty(getOptions().getString(MESSAGE_TYPE, null))) {
            LOG.warn("The parquet schema can already be calculated by source info, unnecessary to generate it, exit.");
            // 非常粗暴，但是最直接
            System.exit(0);
        }
    }
}
