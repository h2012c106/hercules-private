package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.serder.KVDer;
import com.xiaohongshu.db.hercules.core.serder.KVSer;

public interface KvSerDerSupplier {

    public void setOptions(GenericOptions options);

    public KVSer<?> getKVSer();

    public KVDer<?> getKVDer();

    public OptionsConf getInputOptionsConf();

    public OptionsConf getOutputOptionsConf();

    public DataTypeConverter<?, ?> getDataTypeConverter();

    public CustomDataTypeManager<?, ?> getCustomDataTypeManager();

    public SchemaNegotiatorContext getSchemaNegotiatorContextAsSource();

    public SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget();

}
