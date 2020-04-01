package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.exceptions.SchemaException;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class SchemaFetcherFactory {

    private static final Map<GenericOptions, BaseSchemaFetcher> cache = new HashMap<>();

    private static <T extends BaseSchemaFetcher> T makeSchemaFetcher(GenericOptions options,
                                                                     Class<T> schemaFetcherClass)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<T> constructor = schemaFetcherClass.getConstructor(options.getClass());
        return constructor.newInstance(options);
    }

    synchronized public static <T extends BaseSchemaFetcher> T getSchemaFetcher(GenericOptions options,
                                                                                Class<T> schemaFetcherClass) {
        // 有可能出现拥有相同的配置项但是其实是两种数据源（schema fetcher）的情况（其实基本99.99%不会出现）
        if (!cache.containsKey(options) || cache.get(options).getClass() != schemaFetcherClass) {
            try {
                cache.put(options, makeSchemaFetcher(options, schemaFetcherClass));
            } catch (Exception e) {
                throw new SchemaException(e);
            }
        }
        return schemaFetcherClass.cast(cache.get(options));
    }

}
