package com.xiaohongshu.db.hercules.mongodb;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.mongodb.mr.input.MongoDBInputFormat;
import com.xiaohongshu.db.hercules.mongodb.mr.output.MongoDBOutputFormat;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOutputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBSchemaFetcher;

public class MongoDBAssemblySupplier extends BaseAssemblySupplier {

    @Override
    public DataSource getDataSource() {
        return new MongoDBDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        return new MongoDBInputOptionsConf();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new MongoDBOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return MongoDBInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return MongoDBOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new MongoDBSchemaFetcher(options);
    }

    @Override
    public MRJobContext getJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }
}
