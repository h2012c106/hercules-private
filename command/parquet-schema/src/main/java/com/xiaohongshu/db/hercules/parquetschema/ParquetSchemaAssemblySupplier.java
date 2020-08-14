package com.xiaohongshu.db.hercules.parquetschema;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHerculesDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHiveDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetSchemaFetcher;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetSqoopDataTypeConverter;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputFormat;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputMRJobContext;
import com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf;
import com.xiaohongshu.db.hercules.parquetschema.schema.ParquetSchemaSchemaNegotiatorContext;


public class ParquetSchemaAssemblySupplier extends BaseAssemblySupplier {

    @Override
    protected DataSource innerGetDataSource() {
        return new ParquetSchemaDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
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
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return null;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return ParquetSchemaOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new ParquetSchemaFetcher(options);
    }

    @Override
    protected MRJobContext innerGetJobContextAsSource() {
        return null;
    }

    @Override
    protected MRJobContext innerGetJobContextAsTarget() {
        return new ParquetSchemaOutputMRJobContext(options);
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return new ParquetSchemaSchemaNegotiatorContext(options);
    }
}
