package com.xiaohongshu.db.hercules.hbase2.MangerFactory;

import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class ManagerFactory {

    private static final Map<GenericOptions, BaseManager> cache = new HashMap<>();

    private static <T extends BaseManager> T makeManager(GenericOptions options,
                                                         Class<T> BaseManagerClass)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<T> constructor = BaseManagerClass.getConstructor(options.getClass());
        return constructor.newInstance(options);
    }

    synchronized public static <T extends BaseManager> T getManager(GenericOptions options,
                                                                                Class<T> BaseManagerClass) {
        // 有可能出现拥有相同的配置项但是其实是两种数据源（schema fetcher）的情况（其实基本99.99%不会出现）
        if (!cache.containsKey(options) || !BaseManagerClass.isAssignableFrom(cache.get(options).getClass())) {
            try {
                cache.put(options, makeManager(options, BaseManagerClass));
            } catch (Exception e) {
                throw new SchemaException(e);
            }
        }
        return BaseManagerClass.cast(cache.get(options));
    }
}
