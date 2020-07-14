package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;

import java.sql.Types;

public class CanalEntryDataTypeConverter extends RDBMSDataTypeConverter {

    @Override
    public Integer convertElementType(DataType type) {
        switch (type.getBaseDataType()) {
            case BYTES:
                return Types.CLOB;
            case BYTE:
                return Types.TINYINT;
            case SHORT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case BOOLEAN:
                return Types.BOOLEAN;
            case STRING:
                return Types.VARCHAR;
            case DATE:
                return Types.DATE;
            case DATETIME:
                return Types.TIMESTAMP;
            case TIME:
                return Types.TIME;
            default:
                throw new RuntimeException("Unknown column type: " + type.getBaseDataType().name());
        }
    }
}
