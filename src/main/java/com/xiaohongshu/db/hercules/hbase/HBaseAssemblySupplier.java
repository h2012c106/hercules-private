package com.xiaohongshu.db.hercules.hbase;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.assembly.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterInitializer;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseInputFormat;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseOutputFormat;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseOutputMRJobContext;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseSchemaFetcher;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;

public class HBaseAssemblySupplier extends BaseAssemblySupplier
        implements HBaseManagerInitializer, DataTypeConverterInitializer<HBaseDataTypeConverter> {

    public HBaseAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return HBaseInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return HBaseOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return new HBaseSchemaFetcher(options, initializeConverter());
    }

    @Override
    protected MRJobContext setJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new HBaseOutputMRJobContext();
    }

    @Override
    public HBaseDataTypeConverter initializeConverter() {
        return new HBaseDataTypeConverter();
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }
}

