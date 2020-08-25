package com.xiaohongshu.db.hercules.serder.map;

import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serder.KVDer;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerDerSupplier;

public class MapSerDerSupplier extends BaseKvSerDerSupplier {
    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVSer<?> innerGetKVSer() {
        return new MapSer();
    }

    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVDer<?> innerGetKVDer() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new TableOptionsConf();
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return null;
    }
}
