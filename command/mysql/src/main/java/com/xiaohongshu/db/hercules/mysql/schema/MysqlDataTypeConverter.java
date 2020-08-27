package com.xiaohongshu.db.hercules.mysql.schema;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.rdbms.schema.ColumnInfo;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Types;

public class MysqlDataTypeConverter extends RDBMSDataTypeConverter {
    @Override
    public DataType convertElementType(ColumnInfo standard) {
        switch (standard.getSqlType()) {
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                // canal认为mysql jdbc会把TEXT归为binary，但测试下来貌似还是varchar，多留个心眼儿吧
                if (StringUtils.endsWithIgnoreCase(standard.getColumnTypeName(), "TINYTEXT")
                        || StringUtils.endsWithIgnoreCase(standard.getColumnTypeName(), "TEXT")
                        || StringUtils.endsWithIgnoreCase(standard.getColumnTypeName(), "MEDIUMTEXT")
                        || StringUtils.endsWithIgnoreCase(standard.getColumnTypeName(), "LONGTEXT")) {
                    return BaseDataType.STRING;
                } else {
                    return BaseDataType.BYTES;
                }
            default:
                return super.convertElementType(standard);
        }
    }
}
