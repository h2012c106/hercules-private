package com.xiaohongshu.db.hercules.tidb;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;
import com.xiaohongshu.db.hercules.tidb.mr.TiDBInputFormat;
import com.xiaohongshu.db.hercules.tidb.mr.TiDBOutputMRJobContext;
import com.xiaohongshu.db.hercules.tidb.option.TiDBInputOptionsConf;

public class TiDBAssemblySupplier extends MysqlAssemblySupplier {

    @Override
    public DataSource getDataSource() {
        return new TiDBDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        return new TiDBInputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return TiDBInputFormat.class;
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return new TiDBOutputMRJobContext();
    }
}
