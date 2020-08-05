package com.xiaohongshu.db.hercules.mongodb;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.mongodb.datatype.MongoDBCustomDataTypeManager;
import com.xiaohongshu.db.hercules.mongodb.mr.input.MongoDBInputFormat;
import com.xiaohongshu.db.hercules.mongodb.mr.output.MongoDBOutputFormat;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOutputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBDataTypeConverter;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBSchemaFetcher;

public class MongoDBAssemblySupplier extends BaseAssemblySupplier {

    @Override
    public DataSource innerGetDataSource() {
        return new MongoDBDataSource();
    }

    @Override
    public OptionsConf innerGetInputOptionsConf() {
        return new MongoDBInputOptionsConf();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new MongoDBOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return MongoDBInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return MongoDBOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher<?> innerGetSchemaFetcher() {
        return new MongoDBSchemaFetcher(options.getOptionsType().getRole());
    }

    @Override
    public MRJobContext innerGetJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public MRJobContext innerGetJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new MongoDBDataTypeConverter();
    }

    @Override
    public CustomDataTypeManager<?, ?> innerGetCustomDataTypeManager() {
        return new MongoDBCustomDataTypeManager();
    }

}
