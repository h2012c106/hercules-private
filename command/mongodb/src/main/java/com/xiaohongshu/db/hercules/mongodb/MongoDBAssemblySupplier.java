package com.xiaohongshu.db.hercules.mongodb;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.context.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
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
    protected DataSource innerGetDataSource() {
        return new MongoDBDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new MongoDBInputOptionsConf();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new MongoDBOutputOptionsConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return MongoDBInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return MongoDBOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new MongoDBSchemaFetcher(options);
    }

    @Override
    protected MRJobContext innerGetJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    protected MRJobContext innerGetJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new MongoDBDataTypeConverter();
    }

    @Override
    protected CustomDataTypeManager<?, ?> innerGetCustomDataTypeManager() {
        return MongoDBCustomDataTypeManager.INSTANCE;
    }

}
