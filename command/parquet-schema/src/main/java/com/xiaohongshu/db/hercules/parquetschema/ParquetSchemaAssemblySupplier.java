package com.xiaohongshu.db.hercules.parquetschema;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;
import com.xiaohongshu.db.hercules.parquet.schema.*;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputFormat;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputMRJobContext;
import com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf;
import com.xiaohongshu.db.hercules.parquetschema.schema.ParquetSchemaSchemaNegotiatorContext;


public class ParquetSchemaAssemblySupplier extends BaseAssemblySupplier {

    @Override
    public DataSource innerGetDataSource() {
        return new ParquetSchemaDataSource();
    }

    @Override
    public OptionsConf innerGetInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new ParquetSchemaOptionsConf();
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        SchemaStyle schemaStyle = SchemaStyle.valueOfIgnoreCase(options.getString(ParquetOptionsConf.SCHEMA_STYLE, null));
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

    @Override
    public Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return null;
    }

    @Override
    public Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return ParquetSchemaOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> innerGetSchemaFetcher() {
        return new ParquetSchemaFetcher(options);
    }

    @Override
    public MRJobContext innerGetJobContextAsSource() {
        return null;
    }

    @Override
    public MRJobContext innerGetJobContextAsTarget() {
        return new ParquetSchemaOutputMRJobContext(options);
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return new ParquetSchemaSchemaNegotiatorContext(options);
    }
}
