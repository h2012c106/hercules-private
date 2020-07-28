package com.xiaohongshu.db.hercules.parquetschema;

import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;
import com.xiaohongshu.db.hercules.parquet.schema.*;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputFormat;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputMRJobContext;
import com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf;


public class ParquetSchemaAssemblySupplier extends BaseAssemblySupplier implements DataTypeConverterGenerator<ParquetDataTypeConverter> {

    @Override
    public DataSource getDataSource() {
        return new ParquetSchemaDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new ParquetSchemaOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return null;
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return ParquetSchemaOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new ParquetSchemaFetcher(options, generateConverter());
    }

    @Override
    public MRJobContext getJobContextAsSource() {
        return null;
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return new ParquetSchemaOutputMRJobContext();
    }

    @Override
    public ParquetDataTypeConverter generateConverter() {
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
}
