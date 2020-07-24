package com.xiaohongshu.db.hercules.core.datasource;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

public enum DataSource {
    RDBMS,
    MySQL,
    TiDB,
    Clickhouse,
    HBase(true),
    MongoDB,
    Parquet,
    ParquetSchema,
    Kafka(true);

    DataSource() {
        this(false);
    }

    DataSource(boolean hasKvConverter) {
        this.hasKvConverter = hasKvConverter;
    }

    private final boolean hasKvConverter;

    public boolean hasKvConverter() {
        return hasKvConverter;
    }
    Myhub;

    public static DataSource valueOfIgnoreCase(String value) {
        for (DataSource dataSource : DataSource.values()) {
            if (StringUtils.equalsIgnoreCase(dataSource.name(), value)) {
                return dataSource;
            }
        }
        throw new ParseException("Illegal data source: " + value);
    }
}
