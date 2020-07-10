package com.xiaohongshu.db.hercules;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.assembly.AssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJob;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.parser.CmdParser;
import com.xiaohongshu.db.hercules.core.parser.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiator;
import com.xiaohongshu.db.hercules.core.utils.LogUtils;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import lombok.SneakyThrows;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class Hercules {

    private static final Log LOG = LogFactory.getLog(Hercules.class);

    private static final String CONFIG_FILE = "hercules.properties";

    private static final String MODULE_DIR = "hercules-module";
    private static final String MODULE_ASSEMBLY_CLASS_PROPERTY_NAME = "hercules.assembly.class";

    @SneakyThrows
    private static void printVersionInfo() {
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.read(new FileReader(Hercules.class.getClassLoader().getResource(CONFIG_FILE).getPath()));
        String version = configuration.getString("hercules.version");
        String buildTime = configuration.getString("hercules.build.time");
        LOG.info(String.format("Current HERCULES version is [%s], built at [%s]", version, buildTime));
    }

    private static AssemblySupplier getAssemblySupplier(String dataSourceName) {
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

    public static void main(String[] args) {
        LogUtils.configureLog4J();

        printVersionInfo();

        // 获得 dataFlow 参数，模式为 SOURCE_TYPE::TARGET_TYPE
        // e.g., MYSQL::TIDB，表示 mysql 导入 tidb
        String dataFlowOption = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        // 获得DataSource类型
        String[] dataSourceNames = ParseUtils.getDataSourceNames(dataFlowOption);
        String sourceDataSourceName = dataSourceNames[0];
        String targetDataSourceName = dataSourceNames[1];

        AssemblySupplier sourceSupplier = getAssemblySupplier(sourceDataSourceName);
        AssemblySupplier targetSupplier = getAssemblySupplier(targetDataSourceName);

        DataSource sourceDataSource = sourceSupplier.getDataSource();
        DataSource targetDataSource = targetSupplier.getDataSource();

        // 获得target、source、common对应的parser
        CmdParser commonParser = new CmdParser(
                new CommonOptionsConf(),
                null,
                null
        );
        CmdParser sourceParser = new CmdParser(
                sourceSupplier.getInputOptionsConf(),
                sourceDataSource,
                OptionsType.SOURCE
        );
        CmdParser targetParser = new CmdParser(
                targetSupplier.getOutputOptionsConf(),
                targetDataSource,
                OptionsType.TARGET
        );

        WrappingOptions wrappingOptions = new WrappingOptions(sourceParser.parse(args),
                targetParser.parse(args),
                commonParser.parse(args));
        sourceSupplier.setOptions(wrappingOptions.getSourceOptions());
        targetSupplier.setOptions(wrappingOptions.getTargetOptions());

        // 处理log-level
        Logger.getRootLogger().setLevel(
                Level.toLevel(
                        wrappingOptions.getCommonOptions().getString(
                                CommonOptionsConf.LOG_LEVEL, CommonOptionsConf.DEFAULT_LOG_LEVEL.toString()
                        )
                )
        );

        LOG.debug("Args: " + StringUtils.join(args, ", "));
        LOG.debug("Options: " + wrappingOptions);

        // 需要打help，则不运行导数行为
        if (commonParser.isHelp() || sourceParser.isHelp() || targetParser.isHelp()) {
            System.exit(0);
        }

        SchemaNegotiator negotiator = new SchemaNegotiator(
                wrappingOptions,
                sourceSupplier.getSchemaFetcher(),
                targetSupplier.getSchemaFetcher(),
                sourceSupplier.getSchemaNegotiatorContextAsSource(),
                targetSupplier.getSchemaNegotiatorContextAsTarget()
        );
        negotiator.negotiate();

        MRJob job = new MRJob(sourceSupplier, targetSupplier, wrappingOptions);

        int ret;
        try {
            job.run(args);
            ret = 0;
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getStackTrace(e));
            ret = 1;
        }
        System.exit(ret);
    }
}
