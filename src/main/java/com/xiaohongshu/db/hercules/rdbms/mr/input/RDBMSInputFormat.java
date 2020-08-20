package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManagerGenerator;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.core.utils.StingyMap;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManagerGenerator;
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

public class RDBMSInputFormat extends HerculesInputFormat
        implements RDBMSManagerGenerator, DataTypeConverterGenerator<RDBMSDataTypeConverter>, CustomDataTypeManagerGenerator {

    private static final Log LOG = LogFactory.getLog(RDBMSInputFormat.class);

    public static final String AVERAGE_MAP_ROW_NUM = "hercules.average.map.row.num";

    protected RDBMSManager manager;
    private String baseSql;
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

        converter = generateConverter();
        manager = generateManager(sourceOptions);
        schemaFetcher = initializeSchemaFetcher(sourceOptions, converter, manager);
        baseSql = SqlUtils.makeBaseQuery(sourceOptions);

        columnTypeMap = SchemaUtils.convert(sourceOptions.getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null), generateCustomDataTypeManager());
        columnTypeMap = new StingyMap<>(columnTypeMap);
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
    public RDBMSManager generateManager(GenericOptions options) {
        return new RDBMSManager(options);
    }

    @Override
    public RDBMSDataTypeConverter generateConverter() {
        return new RDBMSDataTypeConverter();
    }
}