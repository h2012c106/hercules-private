package com.xiaohongshu.db.hercules.core.utils;

import com.xiaohongshu.db.hercules.Hercules;
import com.xiaohongshu.db.hercules.core.assembly.AssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ConfigUtils {

    private static final Log LOG = LogFactory.getLog(ConfigUtils.class);

    private static final String CONFIG_FILE = "hercules.properties";
    private static final String MODULE_DIR = "hercules-module";
    private static final String MODULE_ASSEMBLY_CLASS_PROPERTY_NAME = "hercules.module.assembly.class";

    @SneakyThrows
    public static void printVersionInfo() {
        String propertyFile = "/" + CONFIG_FILE;
        InputStream is = Hercules.class.getResourceAsStream(propertyFile);
        Properties properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Please create the config file to specify the hercules properties, which should be stored as: " + propertyFile, e);
        }
        String version = properties.getProperty("hercules.version");
        String buildTime = properties.getProperty("hercules.build.time");
        LOG.info(String.format("Current HERCULES version is [%s], built at [%s]", version, buildTime));
    }

    public static AssemblySupplier getAssemblySupplier(String dataSourceName) {
        String propertyFile = String.format("/%s/%s.properties", MODULE_DIR, dataSourceName.toLowerCase());
        InputStream is = Hercules.class.getResourceAsStream(propertyFile);
        Properties properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Please create the config file to specify the assembly supplier, which should be stored as: " + propertyFile, e);
        }
        String assemblyClassName = properties.getProperty(MODULE_ASSEMBLY_CLASS_PROPERTY_NAME);
        try {
            return (AssemblySupplier) Class.forName(assemblyClassName).newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("The class [%s] defined in [%s] not exist, please check.", assemblyClassName, propertyFile), e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataSource getDataSource(String dataSourceName) {
        return getAssemblySupplier(dataSourceName).getDataSource();
    }

}
