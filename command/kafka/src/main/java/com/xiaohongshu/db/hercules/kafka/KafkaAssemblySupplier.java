package com.xiaohongshu.db.hercules.kafka;

import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaDataTypeConverter;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaSchemaFetcher;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;

public class KafkaAssemblySupplier extends BaseAssemblySupplier
        implements KafkaManagerInitializer, DataTypeConverterGenerator<KafkaDataTypeConverter> {

    @Override
    public DataSource getDataSource() {
        return new KafkaDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new KafkaOptionConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return KafkaOutPutFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new KafkaSchemaFetcher(options, generateConverter());
    }

    @Override
    public KafkaDataTypeConverter generateConverter() {
        return new KafkaDataTypeConverter();
    }

    @Override
    public KafkaManager initializeManager(GenericOptions options) {
        return new KafkaManager(options);
    }
}
