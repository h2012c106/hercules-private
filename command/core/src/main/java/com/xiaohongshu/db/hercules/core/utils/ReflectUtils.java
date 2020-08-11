package com.xiaohongshu.db.hercules.core.utils;

import com.cloudera.sqoop.util.ClassLoaderStack;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Function;

public final class ReflectUtils {

    private static final Log LOG = LogFactory.getLog(ReflectUtils.class);

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

    public static List<Field> getFiledList(Class<?> clazz) {
        return Arrays.asList(clazz.getDeclaredFields());
    }

    public static <T> T loadJarClass(String jarName, String className, Class<T> clazz) {
        try {
            ClassLoaderStack.addJarFile(jarName, className);
        } catch (IOException e) {
            throw new RuntimeException("Load jar file failed: " + jarName, e);
        }
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            T res = clazz.cast(Class.forName(className, true, cl).newInstance());
            LOG.info(String.format("Load class [%s] from [%s] with [%s] successfully.", className, jarName, cl.toString()));
            return res;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Cannot load class [%s] from class loader [%s].", className, cl.toString()), e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(String.format("The constructor of [%s] need to be public and non-arg.", className), e);
        }
    }

    public static <T> T constructWithNonArgsConstructor(String className, Class<T> clazz) {
        try {
            return clazz.cast(Class.forName(className).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
