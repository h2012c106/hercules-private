package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.context.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RDBMSAssemblySupplier extends BaseAssemblySupplier {

    private static final Log LOG = LogFactory.getLog(RDBMSAssemblySupplier.class);

    @Override
    public DataSource innerGetDataSource() {
        return new RDBMSDataSource();
    }

    @Override
    public OptionsConf innerGetInputOptionsConf() {
        return new RDBMSInputOptionsConf();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new RDBMSOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return RDBMSInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return RDBMSOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> innerGetSchemaFetcher() {
        return new RDBMSSchemaFetcher(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new RDBMSDataTypeConverter();
    }

    @Override
    public MRJobContext innerGetJobContextAsTarget() {
        return new RDBMSOutputMRJobContext(options);
    }

    private RDBMSManager manager = null;

    protected RDBMSManager innerGetManager() {
        return new RDBMSManager(options);
    }

    synchronized public final RDBMSManager getManager() {
        if (manager == null) {
            LOG.debug(String.format("Initializing InputOptionsConf of [%s]...", getClass().getSimpleName()));
            manager = innerGetManager();
            HerculesContext.instance().inject(manager);
        }
        return manager;
    }
}
