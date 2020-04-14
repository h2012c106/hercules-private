package com.xiaohongshu.db.hercules.core.mr;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;

public interface SchemaFetcherGetter<T extends BaseSchemaFetcher> {
    public T getSchemaFetcher(GenericOptions options);
}
