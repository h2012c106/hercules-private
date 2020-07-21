package com.xiaohongshu.db.hercules.myhub.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.myhub.MyhubUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

import java.sql.SQLException;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.IGNORE_SPLIT_KEY_CHECK;

public class MyhubSchemaFetcher extends RDBMSSchemaFetcher {

    private boolean isShard;

    private void initializeIsShard(GenericOptions options, RDBMSManager manager) {
        try {
            this.isShard = MyhubUtils.isShard(options, manager);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public MyhubSchemaFetcher(GenericOptions options, RDBMSDataTypeConverter converter, RDBMSManager manager) {
        super(options, converter, manager);
        initializeIsShard(options, manager);
    }

    public MyhubSchemaFetcher(GenericOptions options, RDBMSManager manager) {
        super(options, manager);
        initializeIsShard(options, manager);
    }

    @Override
    protected String getNoneLineSql(String baseSql) {
        if (isShard) {
            return "/*MYHUB SHARD_NODES:0; SLAVE_PREFER*/" + super.getNoneLineSql(baseSql);
        } else {
            return "/*SLAVE_PREFER*/" + super.getNoneLineSql(baseSql);
        }
    }

    @Override
    public String getPrimaryKey() throws SQLException {
        return null;
    }

    @Override
    public boolean isIndex(String columnName) throws SQLException {
        throw new RuntimeException(String.format("Myhub doesn't support fetch index info yet, please use '%s' to retry.", IGNORE_SPLIT_KEY_CHECK));
    }
}
