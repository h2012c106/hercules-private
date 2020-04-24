package com.xiaohongshu.db.hercules.clickhouse.schema;

import com.xiaohongshu.db.hercules.clickhouse.schema.manager.ClickhouseManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

import java.sql.SQLException;

public class ClickhouseSchemaFetcher extends RDBMSSchemaFetcher {

    public ClickhouseSchemaFetcher(GenericOptions options, RDBMSDataTypeConverter converter, RDBMSManager manager) {
        super(options, converter, manager);
    }

    @Override
    public String getPrimaryKey() throws SQLException {
        throw new UnsupportedOperationException("Clickhouse does't support the primary key.");
    }
}
