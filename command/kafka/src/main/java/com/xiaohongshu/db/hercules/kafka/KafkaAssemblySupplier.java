package com.xiaohongshu.db.hercules.kafka;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.kafka.mr.KafkaOutPutFormat;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaDataTypeConverter;
import com.xiaohongshu.db.hercules.kafka.schema.KafkaSchemaFetcher;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KafkaAssemblySupplier extends BaseAssemblySupplier {

    private static final Log LOG = LogFactory.getLog(KafkaAssemblySupplier.class);

    @Override
    protected DataSource innerGetDataSource() {
        return new KafkaDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new KafkaOptionConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return KafkaOutPutFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new KafkaSchemaFetcher(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new KafkaDataTypeConverter();
    }

    private KafkaManager manager = null;

    protected KafkaManager innerGetManager() {
        return new KafkaManager(options);
    }

    synchronized protected final KafkaManager getManager() {
        if (manager == null) {
            LOG.debug(String.format("Initializing KafkaManager of [%s]...", getClass().getSimpleName()));
            manager = innerGetManager();
            HerculesContext.instance().inject(manager);
        }
        return manager;
    }
}
