package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.serializer.KvSerializer;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class BaseKvSerializerSupplier implements KvSerializerSupplier {

    private static final Log LOG = LogFactory.getLog(BaseKvSerializerSupplier.class);

    protected GenericOptions options;

    @Override
    public void setOptions(GenericOptions options) {
        this.options = options;
        afterSetOptions();
    }

    protected void afterSetOptions() {
    }

    private KvSerializer<?, ?> kvSerializer;

    abstract protected KvSerializer<?, ?> innerGetKvSerializer();

    @Override
    synchronized public final KvSerializer<?, ?> getKvSerializer() {
        if (kvSerializer == null) {
            LOG.info(String.format("Initializing KvSerializer of [%s]...", getClass().getSimpleName()));
            kvSerializer = innerGetKvSerializer();
        }
        return kvSerializer;
    }

    private OptionsConf inputOptionsConf;

    abstract protected OptionsConf innerGetInputOptionsConf();

    @Override
    synchronized public final OptionsConf getInputOptionsConf() {
        if (inputOptionsConf == null) {
            LOG.info(String.format("Initializing InputOptionsConf of [%s]...", getClass().getSimpleName()));
            inputOptionsConf = innerGetInputOptionsConf();
        }
        return inputOptionsConf;
    }

    private OptionsConf outputOptionsConf;

    abstract protected OptionsConf innerGetOutputOptionsConf();

    @Override
    synchronized public final OptionsConf getOutputOptionsConf() {
        if (outputOptionsConf == null) {
            LOG.info(String.format("Initializing OutputOptionsConf of [%s]...", getClass().getSimpleName()));
            outputOptionsConf = innerGetOutputOptionsConf();
        }
        return outputOptionsConf;
    }

    private DataTypeConverter<?, ?> dataTypeConverter;

    abstract protected DataTypeConverter<?, ?> innerGetDataTypeConverter();

    @Override
    synchronized public final DataTypeConverter<?, ?> getDataTypeConverter() {
        if (dataTypeConverter == null) {
            LOG.info(String.format("Initializing DataTypeConverter of [%s]...", getClass().getSimpleName()));
            dataTypeConverter = innerGetDataTypeConverter();
        }
        return dataTypeConverter;
    }

    private CustomDataTypeManager<?, ?> customDataTypeManager;

    abstract protected CustomDataTypeManager<?, ?> innerGetCustomDataTypeManager();

    @Override
    synchronized public final CustomDataTypeManager<?, ?> getCustomDataTypeManager() {
        if (customDataTypeManager == null) {
            LOG.info(String.format("Initializing CustomDataTypeManager of [%s]...", getClass().getSimpleName()));
            customDataTypeManager = innerGetCustomDataTypeManager();
        }
        return customDataTypeManager;
    }
}
