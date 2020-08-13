package com.xiaohongshu.db.hercules.myhub;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.context.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.myhub.mr.input.MyhubInputFormat;
import com.xiaohongshu.db.hercules.myhub.option.MyhubInputOptionsConf;
import com.xiaohongshu.db.hercules.myhub.option.MyhubOutputOptionsConf;
import com.xiaohongshu.db.hercules.myhub.schema.MyhubSchemaFetcher;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;

public class MyhubAssemblySupplier extends MysqlAssemblySupplier {
    @Override
    public DataSource innerGetDataSource() {
        return new MyhubDataSource();
    }

    @Override
    public OptionsConf innerGetInputOptionsConf() {
        return new MyhubInputOptionsConf();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new MyhubOutputOptionsConf();
    }

    @Override
    public BaseSchemaFetcher<?> innerGetSchemaFetcher() {
        return new MyhubSchemaFetcher(options);
    }

    @Override
    public Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return MyhubInputFormat.class;
    }

    @Override
    public SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsSource() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    public SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    public MRJobContext innerGetJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }
}
