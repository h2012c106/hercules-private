package com.xiaohongshu.db.hercules.core.utils;

import com.cloudera.sqoop.util.ClassLoaderStack;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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

    public static boolean doesImplementInterface(Class<?> clazz, Class<?> interfaceClass) {
        for (Class<?> tmpClazz = clazz; tmpClazz != null; tmpClazz = tmpClazz.getSuperclass()) {
            if (Arrays.asList(tmpClazz.getInterfaces()).contains(interfaceClass)) {
                return true;
            }
        }
        return false;
    }

    public static List<String> listJarList(String dirName) {
        List<String> res = new LinkedList<>();
        File dir = new File(dirName);
        for (File file : dir.listFiles()) {
            if (StringUtils.endsWithIgnoreCase(file.getName(), ".jar")) {
                try {
                    res.add(file.getCanonicalPath());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return res;
    }

}
