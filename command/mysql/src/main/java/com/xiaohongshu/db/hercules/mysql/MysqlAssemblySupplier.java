package com.xiaohongshu.db.hercules.mysql;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlInputFormat;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlOutputFormat;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlOutputMRJobContext;
import com.xiaohongshu.db.hercules.mysql.option.MysqlInputOptionsConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOutputOptionsConf;
import com.xiaohongshu.db.hercules.mysql.schema.manager.MysqlManager;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class MysqlAssemblySupplier extends RDBMSAssemblySupplier {

    @Override
    public DataSource getDataSource() {
        return new MysqlDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        return new MysqlInputOptionsConf();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new MysqlOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return MysqlInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return MysqlOutputFormat.class;
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return new MysqlOutputMRJobContext();
    }

    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new MysqlManager(options);
    }
}
