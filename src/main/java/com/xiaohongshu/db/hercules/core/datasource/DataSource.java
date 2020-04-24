package com.xiaohongshu.db.hercules.core.datasource;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

public enum DataSource {
    RDBMS,
    MySQL,
    TiDB,
    Clickhouse,
    HDFSTextFile;

    public static DataSource valueOfIgnoreCase(String value) {
        for (DataSource dataSource : DataSource.values()) {
            if (StringUtils.equalsIgnoreCase(dataSource.name(), value)) {
                return dataSource;
            }
        }
        throw new ParseException("Illegal data source: " + value);
    }
}
