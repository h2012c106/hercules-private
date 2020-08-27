package com.xiaohongshu.db.hercules.parquet;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputFormat;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputMRJobContext;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputFormat;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputMRJobContext;
import com.xiaohongshu.db.hercules.parquet.option.ParquetInputOptionsConf;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOutputOptionsConf;
import com.xiaohongshu.db.hercules.parquet.schema.*;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;

public class ParquetAssemblySupplier extends BaseAssemblySupplier {

    @Override
    protected DataSource innerGetDataSource() {
        return new ParquetDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new ParquetInputOptionsConf();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new ParquetOutputOptionsConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return ParquetInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return ParquetOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new ParquetSchemaFetcher(options);
    }

    @Override
    protected MRJobContext innerGetJobContextAsSource() {
        return new ParquetInputMRJobContext(options);
    }

    @Override
    protected MRJobContext innerGetJobContextAsTarget() {
        return new ParquetOutputMRJobContext(options);
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return new ParquetSchemaNegotiatorContext(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        SchemaStyle schemaStyle = SchemaStyle.valueOfIgnoreCase(options.getString(SCHEMA_STYLE, null));
        switch (schemaStyle) {
            case SQOOP:
                return ParquetSqoopDataTypeConverter.getInstance();
            case HIVE:
                return ParquetHiveDataTypeConverter.getInstance();
            case ORIGINAL:
                return ParquetHerculesDataTypeConverter.getInstance();
            default:
                throw new RuntimeException();
        }
    }
}
