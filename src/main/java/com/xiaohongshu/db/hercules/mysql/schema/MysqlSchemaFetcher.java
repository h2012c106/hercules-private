package com.xiaohongshu.db.hercules.mysql.schema;

import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.schema.manager.MysqlManager;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class MysqlSchemaFetcher extends RDBMSSchemaFetcher {
    public MysqlSchemaFetcher(GenericOptions options) {
        super(options);
    }

    @Override
    protected RDBMSManager setManager() {
        return new MysqlManager(getOptions());
    }
}
