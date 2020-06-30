package com.xiaohongshu.db.hercules.core.datasource;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;
import com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf;
import org.apache.commons.lang3.StringUtils;

public enum DataSource {
    RDBMS(null),
    MySQL(null),
    TiDB(null),
    Clickhouse(null),
    HBase(null),
    MongoDB(null),
    Parquet(ParquetOptionsConf.DIR),
    ParquetSchema(ParquetOptionsConf.DIR);

    public static DataSource valueOfIgnoreCase(String value) {
        for (DataSource dataSource : DataSource.values()) {
            if (StringUtils.equalsIgnoreCase(dataSource.name(), value)) {
                return dataSource;
            }
        }
        throw new ParseException("Illegal data source: " + value);
    }

    /**
     * 代表文件存放位置的变量名，若非文件格式，则为null
     */
    private String filePositionParam;

    DataSource(String filePositionParam) {
        this.filePositionParam = filePositionParam;
    }

    public String getFilePositionParam() {
        return filePositionParam;
    }
}
