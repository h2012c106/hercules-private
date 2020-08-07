package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.utils.StingyMap;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

public class RDBMSInputFormat extends HerculesInputFormat<ResultSet> {

    private static final Log LOG = LogFactory.getLog(RDBMSInputFormat.class);

    public static final String AVERAGE_MAP_ROW_NUM = "hercules.average.map.row.num";

    protected RDBMSManager manager;
    protected String baseSql;
    protected RDBMSSchemaFetcher schemaFetcher;
    protected RDBMSDataTypeConverter converter;
    protected Map<String, DataType> columnTypeMap;

    protected RDBMSSchemaFetcher initializeSchemaFetcher(GenericOptions options,
                                                         RDBMSDataTypeConverter converter,
                                                         RDBMSManager manager) {
        return new RDBMSSchemaFetcher(options, converter, manager);
    }

    @Override
    protected void initializeContext(GenericOptions sourceOptions) {
        super.initializeContext(sourceOptions);

        converter = (RDBMSDataTypeConverter) HerculesContext.getAssemblySupplierPair().getSourceItem().getDataTypeConverter();
        manager = generateManager(sourceOptions);
        schemaFetcher = initializeSchemaFetcher(sourceOptions, converter, manager);
        baseSql = SqlUtils.makeBaseQuery(sourceOptions);

        columnTypeMap = new StingyMap<>(HerculesContext.getSchemaPair().getSourceItem().getColumnTypeMap());
    }

    protected SplitGetter getSplitGetter(GenericOptions options) {
        if (options.getBoolean(RDBMSInputOptionsConf.BALANCE_SPLIT, true)) {
            return new RDBMSBalanceSplitGetter();
        } else {
            return new RDBMSFastSplitterGetter();
        }
    }

    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        SplitUtils.SplitResult splitResult = SplitUtils.split(options, schemaFetcher, numSplits, manager, baseSql,
                columnTypeMap, getSplitGetter(options.getTargetOptions()));

        configuration.setLong(AVERAGE_MAP_ROW_NUM, splitResult.getTotalNum() / splitResult.getInputSplitList().size());

        return splitResult.getInputSplitList();
    }

    @Override
    public HerculesRecordReader<ResultSet> innerCreateRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new RDBMSRecordReader(context, manager);
    }

    @Override
    protected WrapperGetterFactory<ResultSet> createWrapperGetterFactory() {
        return new RDBMSWrapperGetterFactory();
    }

    public RDBMSManager generateManager(GenericOptions options) {
        return new RDBMSManager(options);
    }
}
