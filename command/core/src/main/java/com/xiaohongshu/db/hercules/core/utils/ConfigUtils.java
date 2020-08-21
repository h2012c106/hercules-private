package com.xiaohongshu.db.hercules.core.utils;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Map;
import java.util.Properties;

public final class ConfigUtils {

    private static final Log LOG = LogFactory.getLog(ConfigUtils.class);

    private static final String CONFIG_FILE = "hercules.properties";

    private static final String CONF_DIR = "conf";
    private static final String MODULE_CONF_FILE = "module.yml";

    private static final String MODULE_JAR_DIR = "module-lib";

    @SneakyThrows
    public static void printVersionInfo() {
        String propertyFile = "/" + CONFIG_FILE;
        InputStream is = ConfigUtils.class.getResourceAsStream(propertyFile);
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

    private static final String HERCULES_PATH = System.getenv("HERCULES_PATH");

    public static String getAbsolutePath(String relativePath) {
        // 不能使用class.getResource方法，因为这一波类都会因为hadoop jar被解在/tmp/hadoop-unjar...的文件夹内后重新加载，而非直接从fatjar中加载，故无法找到真正的jar包位置
        // 现在使用环境变量的方式借助外部操作系统加载到jar包位置
        if (StringUtils.isEmpty(HERCULES_PATH)) {
            throw new RuntimeException("Empty [HERCULES_PATH] system env, please set it to core jar posioition.");
        }
        return HERCULES_PATH + "/" + relativePath;
    }

    /**
     * 从jar包目录下conf文件夹内找module.yml文件，采用集中式配置的方法。
     * 不采用类似dataX的分布式由插件自行提供配置文件的理由在于——若插件自行提供在jar包内，那么势必一开始需要加载所有插件jar包(都是fatjar)，
     * 而考虑到目前已遇到的成吨的类冲突问题，全部加载到一起一定不是个好办法，故根据集中化的配置文件动态加载插件jar包为上策。
     *
     * @param dataSourceName
     * @return
     */
    public static ModuleConfig getModuleConfig(String dataSourceName) {
        String confPath = getAbsolutePath(String.format("%s/%s", CONF_DIR, MODULE_CONF_FILE));
        InputStream is = null;
        try {
            is = new FileInputStream(new File(confPath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        Yaml yaml = new Yaml();
        LOG.info(String.format("Reading module config file [%s]...", confPath));
        Map<String, Map<String, String>> conf = yaml.load(is);
        ModuleConfig moduleConf = null;
        for (Map.Entry<String, Map<String, String>> entry : conf.entrySet()) {
            if (StringUtils.equalsIgnoreCase(entry.getKey(), dataSourceName)) {
                moduleConf = ModuleConfig.get(entry.getValue());
                break;
            }
        }
        if (moduleConf == null) {
            throw new RuntimeException(String.format("Cannot find [%s] module registered in %s/%s, registered module: %s",
                    dataSourceName, CONF_DIR, MODULE_CONF_FILE, conf.keySet()));
        }
        String jarFile = getAbsolutePath(String.format("%s/%s", MODULE_JAR_DIR, moduleConf.getJar()));
        // 设置成绝对路径
        moduleConf.setJar(jarFile);
        return moduleConf;
    }

}
