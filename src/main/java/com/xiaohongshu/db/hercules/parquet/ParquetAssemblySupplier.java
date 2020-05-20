package com.xiaohongshu.db.hercules.parquet;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputFormat;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputMRJobContext;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputFormat;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputMRJobContext;
import com.xiaohongshu.db.hercules.parquet.schema.*;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;

public class ParquetAssemblySupplier extends BaseAssemblySupplier implements DataTypeConverterGenerator<ParquetDataTypeConverter> {

    public ParquetAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return ParquetInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return ParquetOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return new ParquetSchemaFetcher(options, generateConverter());
    }

    @Override
    protected MRJobContext setJobContextAsSource() {
        return new ParquetInputMRJobContext();
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
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
