package com.xiaohongshu.db.hercules;

import com.xiaohongshu.db.hercules.common.options.CommonOptionsConf;
import com.xiaohongshu.db.hercules.common.parser.CommonParser;
import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.assembly.AssemblySupplierFactory;
import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.mr.MRJob;
import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseDataSourceParser;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.core.parser.ParserFactory;
import com.xiaohongshu.db.hercules.core.serialize.SchemaChecker;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class Main {

    private static final Log LOG = LogFactory.getLog(Main.class);

    public static void main(String[] args) {
        // 获得例如xxx->yyy的参数
        String dataFlowOption = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        //获得DataSource类型
        DataSource[] dataSources = ParseUtils.getDataSources(dataFlowOption);
        DataSource sourceDataSource = dataSources[0];
        DataSource targetDataSource = dataSources[1];

        // 获得target、source、common对应的parser
        BaseParser commonParer = new CommonParser();
        BaseDataSourceParser sourceParser = ParserFactory.getParser(sourceDataSource, DataSourceRole.SOURCE);
        BaseDataSourceParser targetParser = ParserFactory.getParser(targetDataSource, DataSourceRole.TARGET);

        WrappingOptions wrappingOptions = new WrappingOptions(sourceParser.parse(args),
                targetParser.parse(args),
                commonParer.parse(args));

        LOG.debug("Options: " + wrappingOptions);

        // 处理log-level
        Logger.getRootLogger().setLevel(
                Level.toLevel(
                        wrappingOptions.getCommonOptions().getString(
                                CommonOptionsConf.LOG_LEVEL, CommonOptionsConf.DEFAULT_LOG_LEVEL.toString()
                        )
                )
        );

        BaseAssemblySupplier sourceAssemblySupplier
                = AssemblySupplierFactory.getAssemblySupplier(sourceDataSource, wrappingOptions.getSourceOptions());
        BaseAssemblySupplier targetAssemblySupplier
                = AssemblySupplierFactory.getAssemblySupplier(targetDataSource, wrappingOptions.getTargetOptions());

        SchemaChecker checker = new SchemaChecker(sourceAssemblySupplier.getSchemaFetcher(),
                targetAssemblySupplier.getSchemaFetcher(), wrappingOptions);
        checker.validate();

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
