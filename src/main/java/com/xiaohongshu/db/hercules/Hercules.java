package com.xiaohongshu.db.hercules;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.common.parser.CommonParser;
import com.xiaohongshu.db.hercules.core.assembly.AssemblySupplierFactory;
import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.MRJob;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.core.parser.ParserFactory;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiator;
import com.xiaohongshu.db.hercules.core.utils.LogUtils;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import lombok.SneakyThrows;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.util.Arrays;

public class Hercules {

    private static final Log LOG = LogFactory.getLog(Hercules.class);

    private static final String CONFIG_FILE = "hercules.properties";

    @SneakyThrows
    private static void printVersionInfo() {
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.read(new FileReader(Hercules.class.getClassLoader().getResource(CONFIG_FILE).getPath()));
        String version = configuration.getString("hercules.version");
        String buildTime = configuration.getString("hercules.build.time");
        LOG.info(String.format("Current HERCULES version is [%s], built at [%s]", version, buildTime));
    }

    public static void main(String[] args) {
        LogUtils.configureLog4J();

        printVersionInfo();

        // 获得 dataFlow 参数，模式为 SOURCE_TYPE::TARGET_TYPE
        // e.g., MYSQL::TIDB，表示 mysql 导入 tidb
        String dataFlowOption = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        // 获得DataSource类型
        DataSource[] dataSources = ParseUtils.getDataSources(dataFlowOption);
        DataSource sourceDataSource = dataSources[0];
        DataSource targetDataSource = dataSources[1];

        // 获得target、source、common对应的parser
        BaseParser commonParser = new CommonParser();
        BaseParser sourceParser = ParserFactory.getParser(sourceDataSource, DataSourceRole.SOURCE);
        BaseParser targetParser = ParserFactory.getParser(targetDataSource, DataSourceRole.TARGET);

        WrappingOptions wrappingOptions = new WrappingOptions(sourceParser.parse(args),
                targetParser.parse(args),
                commonParser.parse(args));

        LOG.debug("Options: " + wrappingOptions);

        // 处理log-level
        Logger.getRootLogger().setLevel(
                Level.toLevel(
                        wrappingOptions.getCommonOptions().getString(
                                CommonOptionsConf.LOG_LEVEL, CommonOptionsConf.DEFAULT_LOG_LEVEL.toString()
                        )
                )
        );

        // 需要打help，则不运行导数行为
        if (commonParser.isHelp() || sourceParser.isHelp() || targetParser.isHelp()) {
            System.exit(0);
        }

        BaseAssemblySupplier sourceAssemblySupplier
                = AssemblySupplierFactory.getAssemblySupplier(sourceDataSource, wrappingOptions.getSourceOptions());
        BaseAssemblySupplier targetAssemblySupplier
                = AssemblySupplierFactory.getAssemblySupplier(targetDataSource, wrappingOptions.getTargetOptions());

        SchemaNegotiator negotiator = new SchemaNegotiator(wrappingOptions, sourceAssemblySupplier.getSchemaFetcher(),
                targetAssemblySupplier.getSchemaFetcher());
        negotiator.negotiate();

        MRJob job = new MRJob(sourceAssemblySupplier, targetAssemblySupplier, wrappingOptions);

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
