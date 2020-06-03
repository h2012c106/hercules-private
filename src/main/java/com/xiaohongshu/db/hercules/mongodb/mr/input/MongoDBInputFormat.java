package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.mongodb.datatype.MongoDBCustomDataTypeManager;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBDataTypeConverter;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManagerGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MongoDBInputFormat extends HerculesInputFormat<MongoDBDataTypeConverter>
        implements MongoDBManagerGenerator {

    private static final Log LOG = LogFactory.getLog(MongoDBInputFormat.class);

    public static final String AVERAGE_MAP_ROW_NUM = "hercules.average.map.row.num";

    private static final int MONGO_UNAUTHORIZED_ERR_CODE = 13;
    private static final int MONGO_ILLEGALOP_ERR_CODE = 20;

    private MongoDBManager manager;

    @Override
    protected void initializeContext(GenericOptions sourceOptions) {
        super.initializeContext(sourceOptions);
        manager = generateManager(sourceOptions);
    }

    /**
     * 逻辑抄的DataX mongo CollectionSplitUtil，但是这个策略会无视配置的Query，整表地split，看性能表现如何再考虑改
     *
     * @param context
     * @param numSplits
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        String splitBy = options.getSourceOptions().getString(MongoDBInputOptionsConf.SPLIT_BY, null);
        String databaseStr = options.getSourceOptions().getString(MongoDBOptionsConf.DATABASE, null);
        String collectionStr = options.getSourceOptions().getString(MongoDBOptionsConf.COLLECTION, null);

        MongoClient client = null;
        try {
            client = manager.getConnection();
            MongoDatabase database = client.getDatabase(databaseStr);
            List<InputSplit> res = new ArrayList<>(numSplits);
            if (numSplits == 1) {
                LOG.warn("Map set to 1, only use 1 map.");
                return Collections.singletonList(new MongoDBInputSplit(null, null, splitBy));
            }

            Document result = database.runCommand(new Document("collStats", collectionStr));
            int docCount = result.getInteger("count");
            if (docCount == 0) {
                return res;
            }
            int avgObjSize = 1;
            Object avgObjSizeObj = result.get("avgObjSize");
            if (avgObjSizeObj instanceof Integer) {
                avgObjSize = (Integer) avgObjSizeObj;
            } else if (avgObjSizeObj instanceof Double) {
                avgObjSize = ((Double) avgObjSizeObj).intValue();
            }
            int splitPointCount = numSplits - 1;
            int chunkDocCount = docCount / numSplits;
            ArrayList<Object> splitPoints = new ArrayList<Object>();

            // test if user has splitVector role(clusterManager)
            boolean supportSplitVector = true;
            try {
                database.runCommand(new Document("splitVector", databaseStr + "." + collectionStr)
                        .append("keyPattern", new Document(splitBy, 1))
                        .append("force", true));
            } catch (MongoCommandException e) {
                if (e.getErrorCode() == MONGO_UNAUTHORIZED_ERR_CODE ||
                        e.getErrorCode() == MONGO_ILLEGALOP_ERR_CODE) {
                    supportSplitVector = false;
                }
            }

            if (supportSplitVector) {
                boolean forceMedianSplit = false;
                int maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
                //int maxChunkSize = (chunkDocCount - 1) * 2 * avgObjSize / (1024 * 1024);
                if (maxChunkSize < 1) {
                    forceMedianSplit = true;
                }
                if (!forceMedianSplit) {
                    LOG.info("Split by splitVector without forceMedianSplit.");
                    result = database.runCommand(new Document("splitVector", databaseStr + "." + collectionStr)
                            .append("keyPattern", new Document(splitBy, 1))
                            .append("maxChunkSize", maxChunkSize)
                            .append("maxSplitPoints", numSplits - 1));
                } else {
                    LOG.info("Split by splitVector with forceMedianSplit.");
                    result = database.runCommand(new Document("splitVector", databaseStr + "." + collectionStr)
                            .append("keyPattern", new Document(splitBy, 1))
                            .append("force", true));
                }
                ArrayList<Document> splitKeys = result.get("splitKeys", ArrayList.class);

                for (Document splitKey : splitKeys) {
                    Object id = splitKey.get(splitBy);
                    splitPoints.add(id);
                }
            } else {
                LOG.info("Split by query.");

                String query = options.getSourceOptions().getString(MongoDBInputOptionsConf.QUERY, null);
                Document findQuery = new Document();
                if (!StringUtils.isEmpty(query)) {
                    findQuery = Document.parse(query);
                }

                Document projectionQuery = new Document();
                // 先把_id置0，就算真的按照_id，也会在下一行置1
                projectionQuery.put(MongoDBManager.ID, 0);
                projectionQuery.put(splitBy, 1);

                int skipCount = chunkDocCount;
                MongoCollection<Document> col = database.getCollection(collectionStr);

                for (int i = 0; i < splitPointCount; i++) {
                    Document doc = col.find(findQuery)
                            .projection(projectionQuery)
                            .sort(new Document(splitBy, 1))
                            .skip(skipCount)
                            .limit(1)
                            .first();
                    Object id = doc.get(splitBy);
                    splitPoints.add(id);
                    skipCount += chunkDocCount;
                }
            }

            Object lastObjectId = null;
            Class splitClass = null;
            for (Object splitPoint : splitPoints) {
                if (splitPoint == null) {
                    throw new MapReduceException("Why null value exists in the split points?!");
                }
                // 检查splitBy列的类型是否一致，不一致会导致漏数
                // 举个例子a列值为[0,1,2,3,4,5,'a','b','c','d']，当取到的point为[3,'b']时，最后这之间左闭右开的数据都会被漏掉
                if (splitClass == null) {
                    splitClass = splitPoint.getClass();
                } else {
                    if (splitClass != splitPoint.getClass()) {
                        throw new MapReduceException(String.format("Unequaled split points type: %s vs %s",
                                splitClass.getCanonicalName(), splitPoint.getClass().getCanonicalName()));
                    }
                }
                Object min = lastObjectId;
                lastObjectId = splitPoint;
                Object max = lastObjectId;
                res.add(new MongoDBInputSplit(min, max, splitBy));
            }
            res.add(new MongoDBInputSplit(lastObjectId, null, splitBy));

            LOG.info(String.format("Actually split to %d splits: %s", res.size(), res.toString()));

            // docCount的值是全表值，有query时不准确，不过也就是颗糖，没必要耗费太多性能在这上面
            configuration.setLong(AVERAGE_MAP_ROW_NUM, docCount / res.size());

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
    protected HerculesRecordReader<Document, MongoDBDataTypeConverter> innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordReader(converter, generateCustomDataTypeManager(), manager);
    }

    @Override
    public MongoDBDataTypeConverter generateConverter() {
        return new MongoDBDataTypeConverter();
    }

    @Override
    public MongoDBManager generateManager(GenericOptions options) {
        return new MongoDBManager(options);
    }

    @Override
    public MongoDBCustomDataTypeManager generateCustomDataTypeManager() {
        return MongoDBCustomDataTypeManager.INSTANCE;
    }
}
