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
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;

public class MyhubAssemblySupplier extends MysqlAssemblySupplier {
    @Override
    public DataSource innerGetDataSource() {
        return new MyhubDataSource();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new MyhubOutputOptionsConf();
    }

    @Override
    public BaseSchemaFetcher<?> innerGetSchemaFetcher() {
        return new MyhubSchemaFetcher(options, (RDBMSDataTypeConverter) getDataTypeConverter(), generateManager(options));
    }

    @Override
    public Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return MyhubInputFormat.class;
    }
}
