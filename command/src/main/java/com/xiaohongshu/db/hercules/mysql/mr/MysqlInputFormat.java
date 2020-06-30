package com.xiaohongshu.db.hercules.mysql.mr;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.schema.manager.MysqlManager;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class MysqlInputFormat extends RDBMSInputFormat {
    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new MysqlManager(options);
    }
}
