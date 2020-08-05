package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class BaseAssemblySupplier implements AssemblySupplier {

    private static final Log LOG = LogFactory.getLog(BaseAssemblySupplier.class);

    protected GenericOptions options;

    @Override
    public void setOptions(GenericOptions options) {
        this.options = options;
        afterSetOptions();
    }

    protected void afterSetOptions() {
    }

    private DataSource dataSource;

    abstract protected DataSource innerGetDataSource();

    @Override
    synchronized public final DataSource getDataSource() {
        if (dataSource == null) {
            LOG.info(String.format("Initializing DataSource of [%s]...", getClass().getSimpleName()));
            dataSource = innerGetDataSource();
        }
        return dataSource;
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

    private Class<? extends HerculesInputFormat<?>> inputFormatClass;

    abstract protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass();

    @Override
    synchronized public final Class<? extends HerculesInputFormat<?>> getInputFormatClass() {
        if (inputFormatClass == null) {
            LOG.info(String.format("Initializing InputFormatClass of [%s]...", getClass().getSimpleName()));
            inputFormatClass = innerGetInputFormatClass();
        }
        return inputFormatClass;
    }

    private Class<? extends HerculesOutputFormat<?>> outputFormatClass;

    abstract protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass();

    @Override
    synchronized public final Class<? extends HerculesOutputFormat<?>> getOutputFormatClass() {
        if (outputFormatClass == null) {
            LOG.info(String.format("Initializing OutputFormatClass of [%s]...", getClass().getSimpleName()));
            outputFormatClass = innerGetOutputFormatClass();
        }
        return outputFormatClass;
    }

    private SchemaFetcher schemaFetcher = null;

    abstract protected SchemaFetcher innerGetSchemaFetcher();

    @Override
    synchronized public final SchemaFetcher getSchemaFetcher() {
        if (schemaFetcher == null) {
            LOG.info(String.format("Initializing SchemaFetcher of [%s]...", getClass().getSimpleName()));
            schemaFetcher = innerGetSchemaFetcher();
        }
        return schemaFetcher;
    }

    private MRJobContext jobContextAsSource;

    protected MRJobContext innerGetJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    synchronized public final MRJobContext getJobContextAsSource() {
        if (jobContextAsSource == null) {
            LOG.info(String.format("Initializing JobContextAsSource of [%s]...", getClass().getSimpleName()));
            jobContextAsSource = innerGetJobContextAsSource();
        }
        return jobContextAsSource;
    }

    private MRJobContext jobContextAsTarget;

    protected MRJobContext innerGetJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    synchronized public final MRJobContext getJobContextAsTarget() {
        if (jobContextAsTarget == null) {
            LOG.info(String.format("Initializing JobContextAsTarget of [%s]...", getClass().getSimpleName()));
            jobContextAsTarget = innerGetJobContextAsTarget();
        }
        return jobContextAsTarget;
    }

    private SchemaNegotiatorContext schemaNegotiatorContextAsSource;

    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsSource() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    synchronized public final SchemaNegotiatorContext getSchemaNegotiatorContextAsSource() {
        if (schemaNegotiatorContextAsSource == null) {
            LOG.info(String.format("Initializing SchemaNegotiatorContextAsSource of [%s]...", getClass().getSimpleName()));
            schemaNegotiatorContextAsSource = innerGetSchemaNegotiatorContextAsSource();
        }
        return schemaNegotiatorContextAsSource;
    }

    private SchemaNegotiatorContext schemaNegotiatorContextAsTarget;

    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    synchronized public final SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget() {
        if (schemaNegotiatorContextAsTarget == null) {
            LOG.info(String.format("Initializing SchemaNegotiatorContextAsTarget of [%s]...", getClass().getSimpleName()));
            schemaNegotiatorContextAsTarget = innerGetSchemaNegotiatorContextAsTarget();
        }
        return schemaNegotiatorContextAsTarget;
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

    protected CustomDataTypeManager<?, ?> innerGetCustomDataTypeManager() {
        return NullCustomDataTypeManager.INSTANCE;
    }

    @Override
    synchronized public final CustomDataTypeManager<?, ?> getCustomDataTypeManager() {
        if (customDataTypeManager == null) {
            LOG.info(String.format("Initializing CustomDataTypeManager of [%s]...", getClass().getSimpleName()));
            customDataTypeManager = innerGetCustomDataTypeManager();
        }
        return customDataTypeManager;
    }
}
