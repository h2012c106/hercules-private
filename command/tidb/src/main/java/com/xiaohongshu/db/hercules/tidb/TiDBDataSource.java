package com.xiaohongshu.db.hercules.tidb;

import com.xiaohongshu.db.hercules.mysql.MysqlDataSource;

public class TiDBDataSource extends MysqlDataSource {

    @Override
    public String name() {
        return "TiDB";
    }
}
