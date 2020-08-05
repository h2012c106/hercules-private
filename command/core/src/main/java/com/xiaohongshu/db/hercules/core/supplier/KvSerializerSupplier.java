package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.serializer.KvSerializer;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;

public interface KvSerializerSupplier {

    public void setOptions(GenericOptions options);

    public KvSerializer<?, ?> getKvSerializer();

    public OptionsConf getInputOptionsConf();

    public OptionsConf getOutputOptionsConf();

    public DataTypeConverter<?,?> getDataTypeConverter();

    public CustomDataTypeManager<?, ?> getCustomDataTypeManager();

}
