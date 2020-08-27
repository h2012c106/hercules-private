package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.serder.KVDer;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class BaseKvSerDerSupplier implements KvSerDerSupplier {

    private static final Log LOG = LogFactory.getLog(BaseKvSerDerSupplier.class);

    protected GenericOptions options;

    @Override
    public void setOptions(GenericOptions options) {
        this.options = options;
        afterSetOptions();
    }

    protected void afterSetOptions() {
    }

    private KVSer<?> KVSer = null;

    abstract protected KVSer<?> innerGetKVSer();

    @Override
    synchronized public final KVSer<?> getKVSer() {
        if (KVSer == null) {
            LOG.debug(String.format("Initializing KVSer of [%s]...", getClass().getSimpleName()));
            KVSer = innerGetKVSer();
            HerculesContext.instance().inject(KVSer);
        }
        return KVSer;
    }

    private KVDer<?> KVDer = null;

    abstract protected KVDer<?> innerGetKVDer();

    @Override
    synchronized public final KVDer<?> getKVDer() {
        if (KVDer == null) {
            LOG.debug(String.format("Initializing KVSer of [%s]...", getClass().getSimpleName()));
            KVDer = innerGetKVDer();
            HerculesContext.instance().inject(KVDer);
        }
        return KVDer;
    }

    private OptionsConf inputOptionsConf = null;

    abstract protected OptionsConf innerGetInputOptionsConf();

    @Override
    synchronized public final OptionsConf getInputOptionsConf() {
        if (inputOptionsConf == null) {
            LOG.debug(String.format("Initializing InputOptionsConf of [%s]...", getClass().getSimpleName()));
            inputOptionsConf = innerGetInputOptionsConf();
        }
        return inputOptionsConf;
    }

    private OptionsConf outputOptionsConf = null;

    abstract protected OptionsConf innerGetOutputOptionsConf();

    @Override
    synchronized public final OptionsConf getOutputOptionsConf() {
        if (outputOptionsConf == null) {
            LOG.debug(String.format("Initializing OutputOptionsConf of [%s]...", getClass().getSimpleName()));
            outputOptionsConf = innerGetOutputOptionsConf();
        }
        return outputOptionsConf;
    }

    private DataTypeConverter<?, ?> dataTypeConverter = null;

    abstract protected DataTypeConverter<?, ?> innerGetDataTypeConverter();

    @Override
    synchronized public final DataTypeConverter<?, ?> getDataTypeConverter() {
        if (dataTypeConverter == null) {
            LOG.debug(String.format("Initializing DataTypeConverter of [%s]...", getClass().getSimpleName()));
            dataTypeConverter = innerGetDataTypeConverter();
            HerculesContext.instance().inject(dataTypeConverter);
        }
        return dataTypeConverter;
    }

    private CustomDataTypeManager<?, ?> customDataTypeManager = null;

    protected CustomDataTypeManager<?, ?> innerGetCustomDataTypeManager() {
        return NullCustomDataTypeManager.INSTANCE;
    }

    @Override
    synchronized public final CustomDataTypeManager<?, ?> getCustomDataTypeManager() {
        if (customDataTypeManager == null) {
            LOG.debug(String.format("Initializing CustomDataTypeManager of [%s]...", getClass().getSimpleName()));
            customDataTypeManager = innerGetCustomDataTypeManager();
        }
        return customDataTypeManager;
    }

    private SchemaNegotiatorContext schemaNegotiatorContextAsSource = null;

    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsSource() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    synchronized public final SchemaNegotiatorContext getSchemaNegotiatorContextAsSource() {
        if (schemaNegotiatorContextAsSource == null) {
            LOG.debug(String.format("Initializing SchemaNegotiatorContextAsSource of [%s]...", getClass().getSimpleName()));
            schemaNegotiatorContextAsSource = innerGetSchemaNegotiatorContextAsSource();
            HerculesContext.instance().inject(schemaNegotiatorContextAsSource);
        }
        return schemaNegotiatorContextAsSource;
    }

    private SchemaNegotiatorContext schemaNegotiatorContextAsTarget = null;

    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    synchronized public final SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget() {
        if (schemaNegotiatorContextAsTarget == null) {
            LOG.debug(String.format("Initializing SchemaNegotiatorContextAsTarget of [%s]...", getClass().getSimpleName()));
            schemaNegotiatorContextAsTarget = innerGetSchemaNegotiatorContextAsTarget();
            HerculesContext.instance().inject(schemaNegotiatorContextAsTarget);
        }
        return schemaNegotiatorContextAsTarget;
    }
}
