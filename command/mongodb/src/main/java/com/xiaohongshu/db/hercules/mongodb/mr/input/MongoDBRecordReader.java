package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.mongodb.MongoDBUtils;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf.BATCH_SIZE;

public class MongoDBRecordReader extends HerculesRecordReader<Document> {

    private static final Log LOG = LogFactory.getLog(MongoDBRecordReader.class);

    private Long pos = 0L;
    /**
     * 用于估算进度
     */
    private Long mapAverageRowNum;

    private MongoCursor<Document> cursor = null;
    private MongoClient client = null;
    private HerculesWritable value;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

    public MongoDBRecordReader(TaskAttemptContext context) {
        super(context);
    }

    private Document makeColumnProjection(List<String> columnNameList) {
        Document res = new Document();
        // 不需要判断，无脑置0，反正如果包含的话也会置回1
        res.put(MongoDBUtils.ID, 0);
        for (String columnName : columnNameList) {
            res.put(columnName, 1);
        }
        return res;
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        mapAverageRowNum = configuration.getLong(MongoDBInputFormat.AVERAGE_MAP_ROW_NUM, 0L);

        String databaseStr = sourceOptions.getString(MongoDBOptionsConf.DATABASE, null);
        String collectionStr = sourceOptions.getString(MongoDBOptionsConf.COLLECTION, null);
        String query = sourceOptions.getString(MongoDBInputOptionsConf.QUERY, null);

        MongoDBInputSplit mongoDBInputSplit = (MongoDBInputSplit) split;
        Document splitQuery = mongoDBInputSplit.getSplitQuery();
        try {
            client = MongoDBUtils.getConnection(sourceOptions);
            MongoCollection<Document> collection = client.getDatabase(databaseStr).withReadPreference(ReadPreference.secondaryPreferred()).getCollection(collectionStr);

            Document filter = splitQuery;
            if (!StringUtils.isEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }

            FindIterable<Document> iterable = collection.find(filter).batchSize(sourceOptions.getInteger(BATCH_SIZE, null));
            if (!schema.getColumnNameList().isEmpty()) {
                Document columnProjection = makeColumnProjection(schema.getColumnNameList());
                iterable.projection(columnProjection);
            }
            cursor = iterable.iterator();
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        try {
            if (!cursor.hasNext()) {
                LOG.info(String.format("Selected %d records.", pos));
                return false;
            }

            ++pos;

            Document item = cursor.next();
            value = new HerculesWritable(((MongoDBWrapperGetterManager) wrapperGetterFactory).documentToMapWrapper(item, null));
            return true;
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        try {
            if (!cursor.hasNext()) {
                return 1.0f;
            } else {
                if (mapAverageRowNum == 0L) {
                    return 0.0f;
                } else {
                    return Math.min(1.0f, pos.floatValue() / mapAverageRowNum.floatValue());
                }
            }
        } catch (Exception e) {
            return 1.0f;
        }
    }

    @Override
    public void innerClose() throws IOException {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception e) {
                LOG.warn("Exception closing cursor: " + ExceptionUtils.getStackTrace(e));
            }
        }
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Exception closing client: " + ExceptionUtils.getStackTrace(e));
            }
        }
    }
}
