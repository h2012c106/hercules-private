package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.filter.pushdown.FilterPushdownJudger;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.mongodb.MongoDBUtils;
import com.xiaohongshu.db.hercules.mongodb.filter.MongoDBFilterPushdownJudger;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.MongoSplitter;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

public class MongoDBInputFormat extends HerculesInputFormat<Document> {

    private static final Log LOG = LogFactory.getLog(MongoDBInputFormat.class);

    public static final String AVERAGE_MAP_ROW_NUM = "hercules.average.map.row.num";

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

    /**
     * @param context
     * @param numSplits
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        String splitBy = sourceOptions.getString(MongoDBInputOptionsConf.SPLIT_BY, null);
        MongoClient client = null;
        try {
            client = MongoDBUtils.getConnection(sourceOptions);
            if (numSplits == 1) {
                LOG.warn("Map set to 1, only use 1 map.");
                return Collections.singletonList(new MongoDBInputSplit(null, null, splitBy));
            }

            String query = sourceOptions.getString(MongoDBInputOptionsConf.QUERY, null);
            Document findQuery = new Document();
            if (!StringUtils.isEmpty(query)) {
                findQuery = Document.parse(query);
            }
            if (getPushdownFilter() != null) {
                findQuery = new Document("$and", Lists.newArrayList(findQuery, ((MongoDBFilterPushdownJudger.KeyValue) getPushdownFilter()).getValue()));
            }
            LOG.info("Use condition to get split min & max: " + findQuery);

            MongoSplitter splitter = MongoSplitterFactory.getSplitter(client, sourceOptions);
            LOG.info("Using `" + splitter.getClass().getSimpleName() + "` to calculate splits.");
            List<InputSplit> res = splitter.calculateSplits(numSplits, splitBy, findQuery);

            // docCount的值是全表值，有query时不准确，不过也就是颗糖，没必要耗费太多性能在这上面
            context.getConfiguration().setLong(AVERAGE_MAP_ROW_NUM, splitter.getCount() / res.size());

            return res;
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    LOG.warn("Exception closing client: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }

    }

    @Override
    protected HerculesRecordReader<Document> innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordReader(context);
    }

    @Override
    protected WrapperGetterFactory<Document> createWrapperGetterFactory() {
        return new MongoDBWrapperGetterManager();
    }

    @Override
    protected FilterPushdownJudger<?> createFilterPushdownJudger() {
        return new MongoDBFilterPushdownJudger();
    }
}
