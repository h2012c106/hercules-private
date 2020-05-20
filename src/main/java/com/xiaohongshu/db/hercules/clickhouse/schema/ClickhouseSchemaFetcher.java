package com.xiaohongshu.db.hercules.clickhouse.schema;

import com.xiaohongshu.db.hercules.clickhouse.schema.manager.ClickhouseManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;

public class ClickhouseSchemaFetcher extends RDBMSSchemaFetcher {

    private static final Log LOG = LogFactory.getLog(ClickhouseSchemaFetcher.class);

    public ClickhouseSchemaFetcher(GenericOptions options, RDBMSDataTypeConverter converter, RDBMSManager manager) {
        super(options, converter, manager);
    }

    @Override
    public String getPrimaryKey() throws SQLException {
        LOG.warn("Clickhouse does't support the primary key.");
        return null;
    }
}
