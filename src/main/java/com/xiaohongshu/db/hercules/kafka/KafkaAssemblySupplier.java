package com.xiaohongshu.db.hercules.kafka;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat;
import com.xiaohongshu.db.hercules.kafka.mr.KafkaOutputMRJobContext;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaDataTypeConverter;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaSchemaFetcher;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;

public class KafkaAssemblySupplier extends BaseAssemblySupplier
        implements KafkaManagerInitializer, DataTypeConverterGenerator<KafkaDataTypeConverter> {

    public KafkaAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return null;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return KafkaOutPutFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return new KafkaSchemaFetcher(options, generateConverter());
    }

    @Override
    protected MRJobContext setJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }


    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new KafkaOutputMRJobContext();
    }

    @Override
    public KafkaDataTypeConverter generateConverter() {
        return new KafkaDataTypeConverter();
    }

    @Override
    public KafkaManager initializeManager(GenericOptions options) {
        return new KafkaManager(options);
    }

    @Override
    protected SchemaNegotiatorContext setSchemaNegotiatorContextAsSource() {
        return new KafkaSchemaNegotiatorContext(options);
    }

}
