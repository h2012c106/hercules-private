package com.xiaohongshu.db.hercules.parquet.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetSchemaNegotiatorContext extends BaseSchemaNegotiatorContext {

    private static final Log LOG = LogFactory.getLog(ParquetSchemaNegotiatorContext.class);

    @Assembly
    private ParquetDataTypeConverter dataTypeConverter;

    public ParquetSchemaNegotiatorContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void afterAll(List<String> columnName, Map<String, DataType> columnType) {
        // 仅在作为下游时装配，上游时一定拿得到
        if (StringUtils.isEmpty(getOptions().getString(MESSAGE_TYPE, null))) {
            MessageType generatedMessageType = dataTypeConverter.convertTypeMap(columnName, columnType);
            if (generatedMessageType != null) {
                String messageTypeStr = generatedMessageType.toString();
                LOG.info("Generate the parquet schema from negotiated column name list and column type map: " + messageTypeStr);
                getOptions().set(MESSAGE_TYPE, messageTypeStr);
            }
        }
    }
}
