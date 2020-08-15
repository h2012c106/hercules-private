package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;

public interface KvSerDerSupplier {

    public void setOptions(GenericOptions options);

    public KvSerDer<?, ?> getKvSerDer();

    public OptionsConf getInputOptionsConf();

    public OptionsConf getOutputOptionsConf();

    public DataTypeConverter<?,?> getDataTypeConverter();

    public CustomDataTypeManager<?, ?> getCustomDataTypeManager();

}
