package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBSchemaFetcher;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManagerGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

public class MongoDBInputFormat extends HerculesInputFormat implements MongoDBManagerGenerator {

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
            MongoDatabase database = client.getDatabase(databaseStr).withReadPreference(ReadPreference.secondaryPreferred());
            List<InputSplit> res = new ArrayList<>(numSplits);
            if (numSplits == 1) {
                LOG.warn("Map set to 1, only use 1 map.");
                return Collections.singletonList(new MongoDBInputSplit(null, null, splitBy));
            }

            Document result = database.runCommand(new Document("collStats", collectionStr));
            int docCount = new BigDecimal(result.get("count").toString()).intValueExact();
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
            List<Object> splitPoints = new ArrayList<>();

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

                // 优先读从库
                MongoCollection<Document> col = database.withReadPreference(ReadPreference.secondaryPreferred()).getCollection(collectionStr);

                // 检查key上是否有索引
                boolean ignoreCheckKey = options.getSourceOptions().getBoolean(MongoDBInputOptionsConf.IGNORE_SPLIT_KEY_CHECK, false);
                if (!ignoreCheckKey && !MongoDBSchemaFetcher.isIndex(col, splitBy)) {
                    throw new RuntimeException(String.format("Cannot specify a non-key split key [%s]. If you insist, please use '--%s'.", splitBy, MongoDBInputOptionsConf.IGNORE_SPLIT_KEY_CHECK));
                }
                // 直接按ObjectId的时间戳切分，没辙了，如果_id不是ObjectId，那只能`--num-mapper 1`
                LOG.info(String.format("Finding min with findQuery: %s; projectionQuery: %s", findQuery, projectionQuery));
                Object minObj = Objects.requireNonNull(col.find(findQuery)
                        .projection(projectionQuery)
                        .sort(new Document(splitBy, 1))
                        .limit(1)
                        .first())
                        .get(splitBy);
                LOG.info(String.format("Finding max with findQuery: %s; projectionQuery: %s", findQuery, projectionQuery));
                Object maxObj = Objects.requireNonNull(col.find(findQuery)
                        .projection(projectionQuery)
                        .sort(new Document(splitBy, -1))
                        .limit(1)
                        .first())
                        .get(splitBy);

                if (minObj.getClass() != maxObj.getClass()) {
                    throw new RuntimeException(String.format("The column [%s] has more than one type, not capable to be a split-by column, min vs max: %s vs %s.",
                            splitBy, minObj.getClass().getCanonicalName(), maxObj.getClass().getCanonicalName()));
                }
                if (minObj.getClass() != ObjectId.class) {
                    throw new RuntimeException("Temporarily only support object id to be a split-by key, other than, use `num-mapper 1`.");
                }
                ObjectId min = (ObjectId) minObj;
                ObjectId max = (ObjectId) maxObj;
                LOG.info(String.format("Min objectId is: %s; Max objectId is: %s.", min, max));
                Date minTs = min.getDate();
                Date maxTs = max.getDate();

                // 从RDBMS复制过来的
                List<BigDecimal> splits = new ArrayList<>();

                BigDecimal decimalMinVal = new BigDecimal(minTs.getTime());
                BigDecimal decimalMaxVal = new BigDecimal(maxTs.getTime());
                BigDecimal decimalNumSplits = BigDecimal.valueOf(numSplits);

                BigDecimal splitSize = tryDivide(decimalMaxVal.subtract(decimalMinVal), (decimalNumSplits));
                if (splitSize.compareTo(MIN_INCREMENT) < 0) {
                    splitSize = MIN_INCREMENT;
                    LOG.warn("Set BigDecimal splitSize to MIN_INCREMENT: " + MIN_INCREMENT.toPlainString());
                }

                BigDecimal curVal = decimalMinVal;

                // min值不可能大于max值，所以数组里至少有一个min
                while (curVal.compareTo(decimalMaxVal) <= 0) {
                    splits.add(curVal);
                    curVal = curVal.add(splitSize);
                }
                // 转换回来
                splitPoints = splits
                        .stream()
                        .map(item -> new ObjectId(new Date(item.toBigInteger().longValueExact())))
                        .collect(Collectors.toList());
                // 确保不漏
                splitPoints.add(0, min);
                splitPoints.add(max);
                // 去掉界外值+去重+排序
                splitPoints = splitPoints.stream()
                        .filter(item -> ((ObjectId) item).compareTo(min) >= 0 && ((ObjectId) item).compareTo(max) <= 0)
                        .distinct()
                        .sorted()
                        .collect(Collectors.toList());


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
    protected HerculesRecordReader<Document> innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordReader(context, manager);
    }

    @Override
    public MongoDBManager generateManager(GenericOptions options) {
        return new MongoDBManager(options);
    }

    private static final BigDecimal MIN_INCREMENT =
            new BigDecimal(10000 * Double.MIN_VALUE);

    protected BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
        try {
            return numerator.divide(denominator);
        } catch (ArithmeticException ae) {
            // 由于ROUND_UP，不可能取到0值
            return numerator.divide(denominator, BigDecimal.ROUND_UP);
        }
    }
}
