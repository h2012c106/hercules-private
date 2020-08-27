package com.xiaohongshu.db.hercules.tidb;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;
import com.xiaohongshu.db.hercules.tidb.mr.TiDBInputFormat;
import com.xiaohongshu.db.hercules.tidb.mr.TiDBOutputMRJobContext;
import com.xiaohongshu.db.hercules.tidb.option.TiDBInputOptionsConf;

public class TiDBAssemblySupplier extends MysqlAssemblySupplier {

    @Override
    protected DataSource innerGetDataSource() {
        return new TiDBDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new TiDBInputOptionsConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return TiDBInputFormat.class;
    }

    @Override
    protected MRJobContext innerGetJobContextAsTarget() {
        return new TiDBOutputMRJobContext(options);
    }
}
