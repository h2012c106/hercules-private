package com.xiaohongshu.db.hercules.core.utils;

import com.cloudera.sqoop.util.ClassLoaderStack;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public final class ReflectUtils {

    private static final Log LOG = LogFactory.getLog(ReflectUtils.class);

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

}
