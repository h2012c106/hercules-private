package com.xiaohongshu.db.hercules.parquetschema;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;
import com.xiaohongshu.db.hercules.parquet.schema.*;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputFormat;
import com.xiaohongshu.db.hercules.parquetschema.mr.ParquetSchemaOutputMRJobContext;
import com.xiaohongshu.db.hercules.parquetschema.schema.ParquetSchemaSchemaNegotiatorContext;


public class ParquetSchemaAssemblySupplier extends BaseAssemblySupplier implements DataTypeConverterGenerator<ParquetDataTypeConverter> {
    public ParquetSchemaAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return null;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return ParquetSchemaOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return new ParquetSchemaFetcher(options, generateConverter());
    }

    @Override
    protected MRJobContext setJobContextAsSource() {
        return null;
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
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

    @Override
    protected SchemaNegotiatorContext setSchemaNegotiatorContextAsTarget() {
        return new ParquetSchemaSchemaNegotiatorContext(options);
    }
}
