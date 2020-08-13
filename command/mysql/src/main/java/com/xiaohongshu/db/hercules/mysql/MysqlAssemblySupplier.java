package com.xiaohongshu.db.hercules.mysql;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.mysql.option.MysqlInputOptionsConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOutputOptionsConf;
import com.xiaohongshu.db.hercules.mysql.schema.MysqlSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.mysql.schema.manager.MysqlManager;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class MysqlAssemblySupplier extends RDBMSAssemblySupplier {

    @Override
    public DataSource innerGetDataSource() {
        return new MysqlDataSource();
    }

    @Override
    public OptionsConf innerGetInputOptionsConf() {
        return new MysqlInputOptionsConf();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new MysqlOutputOptionsConf();
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsSource() {
        return new MysqlSchemaNegotiatorContext(options);
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return new MysqlSchemaNegotiatorContext(options);
    }

    @Override
    protected RDBMSManager innerGetManager() {
        return new MysqlManager(options);
    }
}
