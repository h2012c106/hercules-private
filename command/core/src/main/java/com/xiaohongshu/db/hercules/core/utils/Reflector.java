package com.xiaohongshu.db.hercules.core.utils;

import com.cloudera.sqoop.util.ClassLoaderStack;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

public class Reflector {

    private static final Log LOG = LogFactory.getLog(Reflector.class);

    private final ClassLoader classLoader;

    private final List<String> jarListLoaded;

    public Reflector() {
        classLoader = getClass().getClassLoader();
        jarListLoaded = Collections.emptyList();
    }

    public Reflector(List<String> jarListToLoad) {
        LOG.info("Initialize ClassLoader for: " + jarListToLoad);
        classLoader = URLClassLoader.newInstance(jarListToLoad.stream().map(jarName -> {
            try {
                return new URL("jar:" + new File(jarName).toURI().toURL() + "!/");
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }).toArray(URL[]::new), getClass().getClassLoader());
        // 由于hadoop在加载input/outputFomrat的时候默认加载器为Thread.currentThread().getContextClassLoader()，
        // 若取不到才会用自身类加载器，故这里要置一下，不然会ClassNotFound
        Thread.currentThread().setContextClassLoader(classLoader);
        jarListLoaded = jarListToLoad;
    }

    public List<String> getJarListLoaded() {
        return jarListLoaded;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Deprecated
    public <T> T loadJarClass(String jarName, String className, Class<T> clazz) {
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

    public <T> T constructWithNonArgsConstructor(String className, Class<T> clazz) {
        try {
            return clazz.cast(Class.forName(className, true, classLoader).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
