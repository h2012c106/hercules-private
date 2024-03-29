package com.xiaohongshu.db.hercules;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJob;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.parser.CmdParser;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiator;
import com.xiaohongshu.db.hercules.core.supplier.AssemblySupplier;
import com.xiaohongshu.db.hercules.core.supplier.KvSerDerSupplier;
import com.xiaohongshu.db.hercules.core.utils.ConfigUtils;
import com.xiaohongshu.db.hercules.core.utils.LogUtils;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.core.utils.config.ModuleConfig;
import com.xiaohongshu.db.hercules.core.utils.context.Family;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.reflect.ReflectUtils;
import com.xiaohongshu.db.hercules.core.utils.reflect.Reflector;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class Hercules {

    private static final Log LOG = LogFactory.getLog(Hercules.class);

    private static final String LIB_DIR = "lib";

    private static List<String> getJarList(ModuleConfig sourceModuleConfig, ModuleConfig targetModuleConfig) {
        List<String> res = ReflectUtils.listJarList(ConfigUtils.getAbsolutePath(LIB_DIR));
        res.add(sourceModuleConfig.getJar());
        if (!StringUtils.equals(sourceModuleConfig.getJar(), targetModuleConfig.getJar())) {
            res.add(targetModuleConfig.getJar());
        }
        return res;
    }

    @SneakyThrows
    public static void main(String[] args) {
        Thread.sleep(10000);

        LogUtils.configureLog4J();

        ConfigUtils.printVersionInfo();

        // 获得 dataFlow 参数，模式为 SOURCE_TYPE::TARGET_TYPE
        // e.g., MYSQL::TIDB，表示 mysql 导入 tidb
        String dataFlowOption = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        // 获得DataSource类型
        String[] dataSourceNames = ParseUtils.getDataSourceNames(dataFlowOption);
        String sourceDataSourceName = dataSourceNames[0];
        String targetDataSourceName = dataSourceNames[1];

        ModuleConfig sourceModuleConfig = ConfigUtils.getModuleConfig(sourceDataSourceName);
        ModuleConfig targetModuleConfig = ConfigUtils.getModuleConfig(targetDataSourceName);

        Reflector reflector = new Reflector(getJarList(sourceModuleConfig, targetModuleConfig));

        AssemblySupplier sourceSupplier = reflector.constructWithNonArgsConstructor(
                sourceModuleConfig.getAssemblyClass(),
                AssemblySupplier.class
        );
        AssemblySupplier targetSupplier = reflector.constructWithNonArgsConstructor(
                targetModuleConfig.getAssemblyClass(),
                AssemblySupplier.class
        );

        DataSource sourceDataSource = sourceSupplier.getDataSource();
        DataSource targetDataSource = targetSupplier.getDataSource();

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
        CmdParser commonParser = new CmdParser(
                new CommonOptionsConf(),
                null,
                OptionsType.COMMON
        );

        GenericOptions sourceOptions = sourceParser.parse(args);
        GenericOptions targetOptions = targetParser.parse(args);
        GenericOptions commonOptions = commonParser.parse(args);

        WrappingOptions wrappingOptions = new WrappingOptions(commonOptions, sourceOptions, targetOptions);

        String[] finalArgs = args;
        HerculesContext context = HerculesContext.initialize(
                wrappingOptions,
                sourceSupplier,
                targetSupplier,
                reflector,
                new Function<Family<KvSerDerSupplier>, Void>() {
                    @Override
                    public Void apply(Family<KvSerDerSupplier> serDerSupplierFamily) {
                        KvSerDerSupplier sourceKvSerDerSupplier;
                        if ((sourceKvSerDerSupplier = serDerSupplierFamily.getDerItem()) != null) {
                            wrappingOptions.add(new CmdParser(
                                    sourceKvSerDerSupplier.getInputOptionsConf(),
                                    sourceDataSource,
                                    OptionsType.DER
                            ).parse(finalArgs));
                        }
                        KvSerDerSupplier targetKvSerDerSupplier;
                        if ((targetKvSerDerSupplier = serDerSupplierFamily.getSerItem()) != null) {
                            wrappingOptions.add(new CmdParser(
                                    targetKvSerDerSupplier.getOutputOptionsConf(),
                                    targetDataSource,
                                    OptionsType.SER
                            ).parse(finalArgs));
                        }
                        return null;
                    }
                }
        );

        // 处理log-level
        Logger.getRootLogger().setLevel(
                Level.toLevel(
                        wrappingOptions.getCommonOptions().getString(
                                CommonOptionsConf.LOG_LEVEL, CommonOptionsConf.DEFAULT_LOG_LEVEL.toString()
                        )
                )
        );

        LOG.debug("Options: " + wrappingOptions);

        // 需要打help，则不运行导数行为
        if (commonParser.isHelp() || sourceParser.isHelp() || targetParser.isHelp()) {
            System.exit(0);
        }

        SchemaNegotiator schemaNegotiator = new SchemaNegotiator();
        context.inject(schemaNegotiator);
        schemaNegotiator.negotiate();

        MRJob job = new MRJob(wrappingOptions);
        context.inject(job);
        job.setJarList(reflector.getJarListLoaded());

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
