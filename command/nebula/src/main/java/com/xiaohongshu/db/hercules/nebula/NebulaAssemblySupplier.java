package com.xiaohongshu.db.hercules.nebula;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.nebula.datatype.NebulaCustomDataTypeManager;
import com.xiaohongshu.db.hercules.nebula.mr.output.NebulaOutputFormat;
import com.xiaohongshu.db.hercules.nebula.option.NebulaOutputOptionsConf;
import com.xiaohongshu.db.hercules.nebula.schema.NebulaDataTypeConverter;
import com.xiaohongshu.db.hercules.nebula.schema.NebulaSchemaFetcher;

public class NebulaAssemblySupplier extends BaseAssemblySupplier {
    @Override
    protected DataSource innerGetDataSource() {
        return new NebulaDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return null;
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new NebulaOutputOptionsConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return null;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return NebulaOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new NebulaSchemaFetcher(options);
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new NebulaDataTypeConverter();
    }

    @Override
    protected CustomDataTypeManager<?, ?> innerGetCustomDataTypeManager() {
        return new NebulaCustomDataTypeManager();
    }
}
