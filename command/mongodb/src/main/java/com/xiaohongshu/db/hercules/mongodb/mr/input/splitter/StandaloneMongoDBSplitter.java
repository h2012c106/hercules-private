package com.xiaohongshu.db.hercules.mongodb.mr.input.splitter;

import com.mongodb.MongoClient;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StandaloneMongoDBSplitter extends MongoDBSplitter {

    private static final Log LOG = LogFactory.getLog(StandaloneMongoDBSplitter.class);

    public StandaloneMongoDBSplitter(GenericOptions options, MongoClient client, Document collStats) {
        super(options, client, collStats);
    }

    @Override
    public List<InputSplit> calculateSplits(int numSplits, String splitBy, Document findQuery) {
        int docCount = new BigDecimal(getCollStats().get("count").toString()).intValueExact();
        if (docCount == 0) {
            return Collections.emptyList();
        }
        int avgObjSize = 1;
        Object avgObjSizeObj = getCollStats().get("avgObjSize");
        if (avgObjSizeObj instanceof Integer) {
            avgObjSize = (Integer) avgObjSizeObj;
        } else if (avgObjSizeObj instanceof Double) {
            avgObjSize = ((Double) avgObjSizeObj).intValue();
        }
        int splitPointCount = numSplits - 1;

        boolean forceMedianSplit = false;
        int maxChunkSize = (docCount / splitPointCount) * 2 * avgObjSize / (1024 * 1024);
        if (maxChunkSize < 1) {
            forceMedianSplit = true;
        }

        Document splitVectorResult;
        if (!forceMedianSplit) {
            LOG.info("Split by splitVector without forceMedianSplit.");
            splitVectorResult = getDatabase().runCommand(new Document("splitVector", getCollection().getNamespace().getFullName())
                    .append("keyPattern", new Document(splitBy, 1))
                    .append("maxChunkSize", maxChunkSize)
                    .append("maxSplitPoints", splitPointCount));
        } else {
            LOG.info("Split by splitVector with forceMedianSplit.");
            splitVectorResult = getDatabase().runCommand(new Document("splitVector", getCollection().getNamespace().getFullName())
                    .append("keyPattern", new Document(splitBy, 1))
                    .append("force", true));
        }
        ArrayList<Document> splitKeys = splitVectorResult.get("splitKeys", ArrayList.class);

        Object previous = null;
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
        for (Document splitKey : splitKeys) {
            Object splitPoint = splitKey.get(splitBy);
            splits.add(createSplitFromBounds(previous, splitPoint, splitBy));
            previous = splitPoint;
        }
        splits.add(createSplitFromBounds(previous, null, splitBy));

        return filterEmptySplits(splits, findQuery);
    }
}
