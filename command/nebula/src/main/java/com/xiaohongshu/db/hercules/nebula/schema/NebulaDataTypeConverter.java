package com.xiaohongshu.db.hercules.nebula.schema;

import com.vesoft.nebula.client.graph.ResultSet;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.nebula.datatype.VidCustomDataType;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

public class NebulaDataTypeConverter implements DataTypeConverter<String, ResultSet.Result> {
    @Override
    public DataType convertElementType(String standard) {
        if (StringUtils.equalsIgnoreCase(standard, "int")) {
            return BaseDataType.INTEGER;
        } else if (StringUtils.equalsIgnoreCase(standard, "double")) {
            return BaseDataType.DOUBLE;
        } else if (StringUtils.equalsIgnoreCase(standard, "bool")) {
            return BaseDataType.BOOLEAN;
        } else if (StringUtils.equalsIgnoreCase(standard, "string")) {
            return BaseDataType.STRING;
        } else if (StringUtils.equalsIgnoreCase(standard, "timestamp")) {
            return BaseDataType.DATETIME;
        } else if (StringUtils.equalsIgnoreCase(standard, "vid")) {
            return VidCustomDataType.INSTANCE;
        } else {
            throw new RuntimeException("Unknown nebula datatype: " + standard);
        }
    }

    @Override
    public Map<String, DataType> convertRowType(ResultSet.Result line) {
        throw new UnsupportedOperationException();
    }
}
