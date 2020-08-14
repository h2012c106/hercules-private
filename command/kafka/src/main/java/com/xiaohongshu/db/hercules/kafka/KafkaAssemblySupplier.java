package com.xiaohongshu.db.hercules.kafka;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaDataTypeConverter;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaSchemaFetcher;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KafkaAssemblySupplier extends BaseAssemblySupplier
        implements KafkaManagerInitializer, DataTypeConverterGenerator<KafkaDataTypeConverter> {

    private static final Log LOG = LogFactory.getLog(KafkaAssemblySupplier.class);

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
    public SchemaFetcher getSchemaFetcher() {
        return new KafkaSchemaFetcher(options, generateConverter());
    }

    @Override
    public KafkaDataTypeConverter generateConverter() {
        return new KafkaDataTypeConverter();
    }

    private KafkaManager manager = null;

    protected KafkaManager innerGetManager() {
        return new KafkaManager(options);
    }

    synchronized public final KafkaManager getManager() {
        if (manager == null) {
            LOG.debug(String.format("Initializing KafkaManager of [%s]...", getClass().getSimpleName()));
            manager = innerGetManager();
            HerculesContext.instance().inject(manager);
        }
        return manager;
    }
}
