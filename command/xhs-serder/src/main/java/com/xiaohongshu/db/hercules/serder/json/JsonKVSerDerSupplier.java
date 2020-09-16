package com.xiaohongshu.db.hercules.serder.json;

import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerDerSupplier;
import com.xiaohongshu.db.hercules.serder.json.options.JsonInputOptionConf;
import com.xiaohongshu.db.hercules.serder.json.options.JsonOutputOptionConf;
import com.xiaohongshu.db.hercules.serder.json.ser.JsonKVSer;

public class JsonKVSerDerSupplier extends BaseKvSerDerSupplier {
    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVSer<?> innerGetKVSer() {
        return new JsonKVSer();
    }

    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVDer<?> innerGetKVDer() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new JsonInputOptionConf();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new JsonOutputOptionConf();
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return null;
    }
}
