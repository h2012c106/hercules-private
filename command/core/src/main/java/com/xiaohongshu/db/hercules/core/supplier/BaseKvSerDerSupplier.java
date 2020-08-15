package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
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

    private KvSerDer<?, ?> kvSerDer = null;

    abstract protected KvSerDer<?, ?> innerGetKvSerDer();

    @Override
    synchronized public final KvSerDer<?, ?> getKvSerDer() {
        if (kvSerDer == null) {
            LOG.debug(String.format("Initializing KvSerDer of [%s]...", getClass().getSimpleName()));
            kvSerDer = innerGetKvSerDer();
            HerculesContext.instance().inject(kvSerDer);
        }
        return kvSerDer;
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

    abstract protected CustomDataTypeManager<?, ?> innerGetCustomDataTypeManager();

    @Override
    synchronized public final CustomDataTypeManager<?, ?> getCustomDataTypeManager() {
        if (customDataTypeManager == null) {
            LOG.debug(String.format("Initializing CustomDataTypeManager of [%s]...", getClass().getSimpleName()));
            customDataTypeManager = innerGetCustomDataTypeManager();
        }
        return customDataTypeManager;
    }
}
