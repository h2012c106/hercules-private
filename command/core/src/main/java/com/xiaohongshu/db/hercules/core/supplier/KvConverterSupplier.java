package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public interface KvConverterSupplier {

    public void setOptions(GenericOptions options);

    public KvConverter<?, ?, ?> getKvConverter();

    public BaseOptionsConf getOutputOptionsConf();

    public BaseOptionsConf getInputOptionsConf();

}
