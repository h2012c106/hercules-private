package com.xiaohongshu.db.hercules.mongodb.mr.input.splitter;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mongodb.MongoDBUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class SampleSplitter extends MongoSplitter {

    private static final Log LOG = LogFactory.getLog(SampleSplitter.class);

    public static final int DEFAULT_SAMPLES_PER_SPLIT = 10;
    public static final int MEANINGFUL_SAMPLE_SIZE = 30;

    public SampleSplitter(GenericOptions options, MongoClient client, Document collStats) {
        super(options, client,collStats);
    }

    @Override
    public List<InputSplit> calculateSplits(int numSplits, String splitBy, Document findQuery) {
        int totalSamples = Math.max(MEANINGFUL_SAMPLE_SIZE, (int) Math.floor(DEFAULT_SAMPLES_PER_SPLIT * numSplits));

        Document projectionQuery = new Document();
        // 先把_id置0，就算真的按照_id，也会在下一行置1
        projectionQuery.put(MongoDBUtils.ID, 0);
        projectionQuery.put(splitBy, 1);
        List<Document> pipeline = Lists.newArrayList(
                new Document("$match", findQuery),
                new Document("$sample", new Document("size", totalSamples)),
                new Document("$project", projectionQuery),
                new Document("$sort", new Document(MongoDBUtils.ID, 1))
        );

        AggregateIterable<Document> aggregateIterable;
        try {
            aggregateIterable = getCollection().aggregate(pipeline);
        } catch (MongoException e) {
            throw new RuntimeException(
                    "Failed to aggregate sample documents. Note that this Splitter "
                            + "implementation is incompatible with MongoDB versions "
                            + "prior to 3.2.", e);
        }
        MongoCursor<Document> iterator = aggregateIterable.iterator();

        Object previous = null;
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
        int i = 0;
        while (iterator.hasNext()) {
            Document sample = iterator.next();
            Object sampleValue = sample.get(splitBy);
            if (i++ % DEFAULT_SAMPLES_PER_SPLIT == 0) {
                splits.add(createSplitFromBounds(previous, sampleValue, splitBy));
                previous = sampleValue;
            }
        }
        splits.add(createSplitFromBounds(previous, null, splitBy));

        return filterEmptySplits(splits, findQuery);
    }
}
