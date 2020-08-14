package com.xiaohongshu.db.hercules.hbase;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseInputFormat;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseOutputFormat;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseOutputMRJobContext;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseSchemaFetcher;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HBaseAssemblySupplier extends BaseAssemblySupplier {

    private static final Log LOG = LogFactory.getLog(HBaseAssemblySupplier.class);

    @Override
    protected DataSource innerGetDataSource() {
        return new HBaseDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new HBaseInputOptionsConf();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new HBaseOutputOptionsConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return HBaseInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return HBaseOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new HBaseSchemaFetcher(options);
    }

    @Override
    protected MRJobContext innerGetJobContextAsTarget() {
        return new HBaseOutputMRJobContext(options);
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsSource() {
        return new HBaseSchemaNegotiatorContext(options);
    }

    @Override
    protected SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return new HBaseSchemaNegotiatorContext(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new HBaseDataTypeConverter();
    }

    private HBaseManager manager = null;

    protected HBaseManager innerGetManager() {
        return new HBaseManager(options);
    }

    synchronized public final HBaseManager getManager() {
        if (manager == null) {
            LOG.debug(String.format("Initializing HBaseManager of [%s]...", getClass().getSimpleName()));
            manager = innerGetManager();
            HerculesContext.instance().inject(manager);
        }
        return manager;
    }

}

