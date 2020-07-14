package com.xiaohongshu.db.hercules.mysql.mr;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.schema.manager.MysqlManager;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class MysqlOutputMRJobContext extends RDBMSOutputMRJobContext {
    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new MysqlManager(options);
    }
}
