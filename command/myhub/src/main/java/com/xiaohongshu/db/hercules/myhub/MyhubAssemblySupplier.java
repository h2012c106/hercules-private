package com.xiaohongshu.db.hercules.myhub;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.myhub.mr.input.MyhubInputFormat;
import com.xiaohongshu.db.hercules.myhub.option.MyhubOutputOptionsConf;
import com.xiaohongshu.db.hercules.myhub.schema.MyhubSchemaFetcher;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;

public class MyhubAssemblySupplier extends MysqlAssemblySupplier {
    @Override
    public DataSource getDataSource() {
        return new MyhubDataSource();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new MyhubOutputOptionsConf();
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new MyhubSchemaFetcher(options, generateConverter(), generateManager(options));
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return MyhubInputFormat.class;
    }
}
