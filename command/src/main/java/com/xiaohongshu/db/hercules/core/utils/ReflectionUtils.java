package com.xiaohongshu.db.hercules.core.utils;

import java.lang.reflect.Method;

public final class ReflectionUtils {

    public static Method getMethod(Class<?> clazz, String methodName, Class<?>... paramClass) throws NoSuchMethodException {
        while (clazz != null) {
            try {
                return clazz.getDeclaredMethod(methodName, paramClass);
            } catch (NoSuchMethodException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchMethodException();
    }

}
