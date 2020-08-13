package com.xiaohongshu.db.hercules.clickhouse.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class ClickhouseSchemaFetcher extends RDBMSSchemaFetcher {

    private static final Log LOG = LogFactory.getLog(ClickhouseSchemaFetcher.class);

    public ClickhouseSchemaFetcher(GenericOptions options) {
        super(options);
    }

    @Override
    protected List<Set<String>> innerGetIndexGroupList() {
        LOG.info("Clickhouse doesn't support index fetching, return empty.");
        return super.innerGetIndexGroupList();
    }

    @Override
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        LOG.info("Clickhouse doesn't support unique key fetching, return empty.");
        return super.innerGetUniqueKeyGroupList();
    }
}
