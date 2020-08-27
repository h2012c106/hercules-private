package com.xiaohongshu.db.hercules.mysql;

import com.xiaohongshu.db.hercules.rdbms.RDBMSDataSource;

public class MysqlDataSource extends RDBMSDataSource {

    @Override
    public String name() {
        return "Mysql";
    }

}
