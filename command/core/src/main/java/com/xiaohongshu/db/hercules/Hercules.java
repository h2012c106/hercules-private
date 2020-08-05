package com.xiaohongshu.db.hercules;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJob;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.parser.CmdParser;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiator;
import com.xiaohongshu.db.hercules.core.supplier.AssemblySupplier;
import com.xiaohongshu.db.hercules.core.supplier.KvSerializerSupplier;
import com.xiaohongshu.db.hercules.core.utils.ConfigUtils;
import com.xiaohongshu.db.hercules.core.utils.LogUtils;
import com.xiaohongshu.db.hercules.core.utils.ModuleConfig;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class Hercules {

    private static final Log LOG = LogFactory.getLog(Hercules.class);

    public static void main(String[] args) {
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
        AssemblySupplier sourceSupplier = ConfigUtils.getAssemblySupplier(sourceModuleConfig);
        AssemblySupplier targetSupplier = ConfigUtils.getAssemblySupplier(targetModuleConfig);

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

        HerculesContext.setWrappingOptions(wrappingOptions);
        HerculesContext.setAssemblySupplierPair(sourceSupplier, targetSupplier);

        sourceSupplier.setOptions(sourceOptions);
        targetSupplier.setOptions(targetOptions);

        KvSerializerSupplier sourceKvSerializerSupplier;
        if (sourceDataSource.hasKvSerializer()
                && (sourceKvSerializerSupplier = HerculesContext.getKvSerializerSupplierPair().getSourceItem()) != null) {
            wrappingOptions.add(new CmdParser(
                    sourceKvSerializerSupplier.getInputOptionsConf(),
                    sourceDataSource,
                    OptionsType.SOURCE_CONVERTER
            ).parse(args));
        }
        KvSerializerSupplier targetKvSerializerSupplier;
        if (targetDataSource.hasKvSerializer()
                && (targetKvSerializerSupplier = HerculesContext.getKvSerializerSupplierPair().getTargetItem()) != null) {
            wrappingOptions.add(new CmdParser(
                    targetKvSerializerSupplier.getOutputOptionsConf(),
                    targetDataSource,
                    OptionsType.TARGET_CONVERTER
            ).parse(args));
        }

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

        SchemaNegotiator.negotiate();

        MRJob job = new MRJob(sourceSupplier, targetSupplier, wrappingOptions);
        job.setJar(sourceModuleConfig.getJar(), targetModuleConfig.getJar());

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
