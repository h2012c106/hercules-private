package com.xiaohongshu.db.hercules.parquet;

import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputFormat;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputMRJobContext;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputFormat;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputMRJobContext;
import com.xiaohongshu.db.hercules.parquet.option.ParquetInputOptionsConf;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOutputOptionsConf;
import com.xiaohongshu.db.hercules.parquet.schema.*;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;

public class ParquetAssemblySupplier extends BaseAssemblySupplier implements DataTypeConverterGenerator<ParquetDataTypeConverter> {

    private ParquetDataTypeConverter converter;

    @Override
    protected void afterSetOptions() {
        converter = generateConverter();
    }

    @Override
    public DataSource getDataSource() {
        return new ParqeutDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        return new ParquetInputOptionsConf();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new ParquetOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return ParquetInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return ParquetOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new ParquetSchemaFetcher(options, converter);
    }

    @Override
    public MRJobContext getJobContextAsSource() {
        return new ParquetInputMRJobContext();
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return new ParquetOutputMRJobContext();
    }

    @Override
    public ParquetDataTypeConverter generateConverter() {
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
