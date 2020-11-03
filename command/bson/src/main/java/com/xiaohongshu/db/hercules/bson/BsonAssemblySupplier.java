package com.xiaohongshu.db.hercules.bson;

import com.xiaohongshu.db.hercules.bson.mr.BsonOutputFormat;
import com.xiaohongshu.db.hercules.bson.mr.BsonOutputMRJobContext;
import com.xiaohongshu.db.hercules.bson.option.BsonOutputOptionsConf;
import com.xiaohongshu.db.hercules.bson.schema.BsonSchemaFetcher;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;

public class BsonAssemblySupplier extends BaseAssemblySupplier {

    protected GenericOptions options;

    @Override
    public void setOptions(GenericOptions options) {
        this.options = options;
        afterSetOptions();
    }

    @Override
    protected DataSource innerGetDataSource() {
        return new BsonDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return null;
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new BsonOutputOptionsConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return null;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return BsonOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new BsonSchemaFetcher(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return null;
    }

    @Override
    protected MRJobContext innerGetJobContextAsTarget() {
        return new BsonOutputMRJobContext(options);
    }
}
