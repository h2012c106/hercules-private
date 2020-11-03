package com.xiaohongshu.db.hercules.elasticsearchv7;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.elasticsearchv7.mr.ElasticsearchOutputFormat;
import com.xiaohongshu.db.hercules.elasticsearchv7.option.ElasticsearchOutputOptionConf;
import com.xiaohongshu.db.hercules.elasticsearchv7.schema.ElasticsearchSchemaFetcher;

public class ElasticsearchAssemblySupplier extends BaseAssemblySupplier {

    @Override
    protected DataSource innerGetDataSource() {
        return new ElasticsearchDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return null;
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new ElasticsearchOutputOptionConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return null;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return ElasticsearchOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new ElasticsearchSchemaFetcher(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return null;
    }
}
