package com.xiaohongshu.db.hercules.redis;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.redis.mr.RedisOutPutFormat;
import com.xiaohongshu.db.hercules.redis.option.RedisOptionConf;
import com.xiaohongshu.db.hercules.redis.schema.RedisDataTypeConverter;
import com.xiaohongshu.db.hercules.redis.schema.RedisSchemaFetcher;
import com.xiaohongshu.db.hercules.redis.schema.manager.RedisManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class RedisAssemblySupplier extends BaseAssemblySupplier {

    private static final Log LOG = LogFactory.getLog(RedisAssemblySupplier.class);

    @Override
    protected DataSource innerGetDataSource() {
        return new RedisDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new RedisOptionConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return RedisOutPutFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new RedisSchemaFetcher(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new RedisDataTypeConverter();
    }

    private RedisManager manager = null;

    protected RedisManager innerGetManager() {
        return new RedisManager(options);
    }

    synchronized protected final RedisManager getManager() {
        if (manager == null) {
            LOG.debug(String.format("Initializing RedisManager of [%s]...", getClass().getSimpleName()));
            manager = innerGetManager();
            HerculesContext.instance().inject(manager);
        }
        return manager;
    }

}
