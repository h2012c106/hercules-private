package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.filter.pushdown.FilterPushdownJudger;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.rdbms.filter.RDBMSFilterPushdownJudger;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;

public class RDBMSInputFormat extends HerculesInputFormat<ResultSet> {

    private static final Log LOG = LogFactory.getLog(RDBMSInputFormat.class);

    public static final String AVERAGE_MAP_ROW_NUM = "hercules.average.map.row.num";

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private RDBMSManager manager;

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private RDBMSSchemaFetcher schemaFetcher;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    protected String baseSql;

    @Override
    public void innerAfterInject() {
        baseSql = manager.makeBaseQuery();
        String filterQuery = (String) getPushdownFilter();
        if (filterQuery != null) {
            baseSql = SqlUtils.addWhere(baseSql, filterQuery);
        }
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
        SplitUtils.SplitResult splitResult = SplitUtils.split(sourceOptions, schema, schemaFetcher, numSplits, manager, baseSql, getSplitGetter(sourceOptions));

        context.getConfiguration().setLong(AVERAGE_MAP_ROW_NUM, splitResult.getTotalNum() / splitResult.getInputSplitList().size());

        return splitResult.getInputSplitList();
    }

    @Override
    public HerculesRecordReader<ResultSet> innerCreateRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new RDBMSRecordReader(context);
    }

    @Override
    protected WrapperGetterFactory<ResultSet> createWrapperGetterFactory() {
        return new RDBMSWrapperGetterFactory();
    }

    @Override
    protected FilterPushdownJudger<?> createFilterPushdownJudger() {
        return new RDBMSFilterPushdownJudger();
    }
}
