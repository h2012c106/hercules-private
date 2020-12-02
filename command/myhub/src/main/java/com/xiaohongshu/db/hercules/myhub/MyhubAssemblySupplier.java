package com.xiaohongshu.db.hercules.myhub;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.context.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.myhub.mr.input.MyhubInputFormat;
import com.xiaohongshu.db.hercules.myhub.mr.output.MyhubOutputFormat;
import com.xiaohongshu.db.hercules.myhub.option.MyhubInputOptionsConf;
import com.xiaohongshu.db.hercules.myhub.option.MyhubOutputOptionsConf;
import com.xiaohongshu.db.hercules.myhub.schema.MyhubSchemaFetcher;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;

public class MyhubAssemblySupplier extends MysqlAssemblySupplier {
    @Override
    protected DataSource innerGetDataSource() {
        return new MyhubDataSource();
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return MyhubOutputFormat.class;
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new MyhubInputOptionsConf();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new MyhubOutputOptionsConf();
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new MyhubSchemaFetcher(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return MyhubInputFormat.class;
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsSource() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    protected MRJobContext innerGetJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }
}
