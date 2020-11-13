package com.xiaohongshu.db.hercules.mongodb.mr.input.splitter;

import com.mongodb.MongoClient;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.mongodb.MongoDBUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SkipMongoDBSplitter extends MongoDBSplitter {

    private static final Log LOG = LogFactory.getLog(SkipMongoDBSplitter.class);

    public SkipMongoDBSplitter(GenericOptions options, MongoClient client, Document collStats) {
        super(options, client, collStats);
    }

    @Override
    public List<InputSplit> calculateSplits(int numSplits, String splitBy, Document findQuery) {
        int docCount = OverflowUtils.numberToInteger(getCollection().countDocuments(findQuery));
        if (docCount == 0) {
            return Collections.emptyList();
        }
        LOG.info(String.format("Total size with query <%s> is: %d", findQuery, docCount));

        int skipStep = Math.max(1, docCount / numSplits);
        LOG.info(String.format("Skip step is: %d", skipStep));

        Document projectionQuery = new Document();
        // 先把_id置0，就算真的按照_id，也会在下一行置1
        projectionQuery.put(MongoDBUtils.ID, 0);
        projectionQuery.put(splitBy, 1);

        Document sort = new Document(splitBy, 1);

        Object previous = null;
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
        for (int i = 1; i < numSplits; ++i) {
            Document tmpSkipDoc = getCollection().find(findQuery)
                    .projection(projectionQuery)
                    .sort(sort)
                    .skip(i * skipStep)
                    .limit(1)
                    .first();
            if (tmpSkipDoc == null) {
                continue;
            }
            Object current = tmpSkipDoc.get(splitBy);
            splits.add(createSplitFromBounds(previous, current, splitBy));
            previous = current;
        }
        splits.add(createSplitFromBounds(previous, null, splitBy));

        return filterEmptySplits(splits, findQuery);
    }

}
