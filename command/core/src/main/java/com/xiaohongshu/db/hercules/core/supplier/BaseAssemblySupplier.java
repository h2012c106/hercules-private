package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.context.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
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

    private DataSource dataSource = null;

    abstract protected DataSource innerGetDataSource();

    @Override
    synchronized public final DataSource getDataSource() {
        if (dataSource == null) {
            LOG.debug(String.format("Initializing DataSource of [%s]...", getClass().getSimpleName()));
            dataSource = innerGetDataSource();
        }
        return dataSource;
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

    private Class<? extends HerculesInputFormat<?>> inputFormatClass = null;

    abstract protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass();

    @Override
    synchronized public final Class<? extends HerculesInputFormat<?>> getInputFormatClass() {
        if (inputFormatClass == null) {
            LOG.debug(String.format("Initializing InputFormatClass of [%s]...", getClass().getSimpleName()));
            inputFormatClass = innerGetInputFormatClass();
        }
        return inputFormatClass;
    }

    private Class<? extends HerculesOutputFormat<?>> outputFormatClass = null;

    abstract protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass();

    @Override
    synchronized public final Class<? extends HerculesOutputFormat<?>> getOutputFormatClass() {
        if (outputFormatClass == null) {
            LOG.debug(String.format("Initializing OutputFormatClass of [%s]...", getClass().getSimpleName()));
            outputFormatClass = innerGetOutputFormatClass();
        }
        return outputFormatClass;
    }

    private SchemaFetcher schemaFetcher = null;

    abstract protected SchemaFetcher innerGetSchemaFetcher();

    @Override
    synchronized public final SchemaFetcher getSchemaFetcher() {
        if (schemaFetcher == null) {
            LOG.debug(String.format("Initializing SchemaFetcher of [%s]...", getClass().getSimpleName()));
            schemaFetcher = innerGetSchemaFetcher();
            HerculesContext.instance().inject(schemaFetcher);
        }
        return schemaFetcher;
    }

    private MRJobContext jobContextAsSource = null;

    protected MRJobContext innerGetJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    synchronized public final MRJobContext getJobContextAsSource() {
        if (jobContextAsSource == null) {
            LOG.debug(String.format("Initializing JobContextAsSource of [%s]...", getClass().getSimpleName()));
            jobContextAsSource = innerGetJobContextAsSource();
            HerculesContext.instance().inject(jobContextAsSource);
        }
        return jobContextAsSource;
    }

    private MRJobContext jobContextAsTarget = null;

    protected MRJobContext innerGetJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    synchronized public final MRJobContext getJobContextAsTarget() {
        if (jobContextAsTarget == null) {
            LOG.debug(String.format("Initializing JobContextAsTarget of [%s]...", getClass().getSimpleName()));
            jobContextAsTarget = innerGetJobContextAsTarget();
            HerculesContext.instance().inject(jobContextAsTarget);
        }
        return jobContextAsTarget;
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
}
