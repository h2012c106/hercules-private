package com.xiaohongshu.db.hercules.rdbms.schema;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Map;

public class RDBMSDataTypeConverter implements DataTypeConverter<ColumnInfo, ResultSet> {

    @Override
    public DataType convertElementType(ColumnInfo standard) {
        int type = standard.getSqlType();
        switch (type) {
            case Types.NULL:
                return BaseDataType.NULL;
            case Types.TINYINT:
                if (standard.isSigned()) {
                    return BaseDataType.BYTE;
                } else {
                    return BaseDataType.SHORT;
                }
            case Types.BIT:
                if (standard.getPrecision() == 1) {
                    return BaseDataType.BYTE;
                } else {
                    return BaseDataType.BYTES;
                }
            case Types.SMALLINT:
                if (standard.isSigned()) {
                    return BaseDataType.SHORT;
                } else {
                    return BaseDataType.INTEGER;
                }
            case Types.INTEGER:
                if (standard.isSigned()) {
                    return BaseDataType.INTEGER;
                } else {
                    return BaseDataType.LONG;
                }
            case Types.BIGINT:
                if (standard.isSigned()) {
                    return BaseDataType.LONG;
                } else {
                    return BaseDataType.LONGLONG;
                }
            case Types.BOOLEAN:
                return BaseDataType.BOOLEAN;
            case Types.REAL:
            case Types.FLOAT:
                if (standard.isSigned()) {
                    return BaseDataType.FLOAT;
                } else {
                    return BaseDataType.DOUBLE;
                }
            case Types.DOUBLE:
                if (standard.isSigned()) {
                    return BaseDataType.DOUBLE;
                } else {
                    return BaseDataType.DECIMAL;
                }
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BaseDataType.DECIMAL;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return BaseDataType.STRING;
            case Types.DATE:
                return BaseDataType.DATE;
            case Types.TIME:
                return BaseDataType.TIME;
            case Types.TIMESTAMP:
                return BaseDataType.DATETIME;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return BaseDataType.BYTES;
            default:
                throw new SchemaException("Unsupported sql type, type code: " + standard);
        }
    }

    /**
     * 这里不用加一级，convert的时候加好了
     *
     * @param type
     * @return
     */
    @Override
    public ColumnInfo getElementType(DataType type) {
        int sqlType;
        switch (type.getBaseDataType()) {
            case BYTES:
                sqlType = Types.BLOB;
                break;
            case BYTE:
                sqlType = Types.TINYINT;
                break;
            case SHORT:
                sqlType = Types.SMALLINT;
                break;
            case INTEGER:
                sqlType = Types.INTEGER;
                break;
            case LONG:
            case LONGLONG:
                sqlType = Types.BIGINT;
                break;
            case FLOAT:
                sqlType = Types.FLOAT;
                break;
            case DOUBLE:
                sqlType = Types.DOUBLE;
                break;
            case DECIMAL:
                sqlType = Types.DECIMAL;
                break;
            case BOOLEAN:
                sqlType = Types.BOOLEAN;
                break;
            case STRING:
                sqlType = Types.VARCHAR;
                break;
            case DATE:
                sqlType = Types.DATE;
                break;
            case TIME:
                sqlType = Types.TIME;
                break;
            case DATETIME:
                sqlType = Types.TIMESTAMP;
                break;
            default:
                throw new RuntimeException("Unknown column type: " + type.getBaseDataType().name());
        }
        return new ColumnInfo(sqlType);
    }

    /**
     * 其实用不到
     *
     * @param line
     * @return
     */
    @Override
    public Map<String, DataType> convertRowType(ResultSet line) {
        throw new UnsupportedOperationException();
    }
}
