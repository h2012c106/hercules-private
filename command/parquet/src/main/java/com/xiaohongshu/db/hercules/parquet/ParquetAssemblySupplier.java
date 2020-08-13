package com.xiaohongshu.db.hercules.parquet;

import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
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
    public DataSource innerGetDataSource() {
        return new ParquetDataSource();
    }

    @Override
    public OptionsConf innerGetInputOptionsConf() {
        return new ParquetInputOptionsConf();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new ParquetOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return ParquetInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return ParquetOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> innerGetSchemaFetcher() {
        return new ParquetSchemaFetcher(options);
    }

    @Override
    public MRJobContext innerGetJobContextAsSource() {
        return new ParquetInputMRJobContext(options);
    }

    @Override
    public MRJobContext innerGetJobContextAsTarget() {
        return new ParquetOutputMRJobContext(options);
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return new ParquetSchemaNegotiatorContext(options);
    }

    @Override
    public DataTypeConverter<?, ?> innerGetDataTypeConverter() {
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
