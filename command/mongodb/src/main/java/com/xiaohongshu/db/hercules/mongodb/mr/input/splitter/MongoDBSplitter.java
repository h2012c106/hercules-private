package com.xiaohongshu.db.hercules.mongodb.mr.input.splitter;

import com.google.common.base.Objects;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mongodb.MongoDBUtils;
import com.xiaohongshu.db.hercules.mongodb.mr.input.MongoDBInputSplit;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class MongoDBSplitter {

    private static final Log LOG = LogFactory.getLog(MongoDBSplitter.class);

    private final GenericOptions options;
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;
    private final Document collStats;

    public static final MinKey MIN_KEY_TYPE = new MinKey();
    public static final MaxKey MAX_KEY_TYPE = new MaxKey();

    public MongoDBSplitter(GenericOptions options, MongoClient client, Document collStats) {
        this.options = options;
        this.client = client;
        this.database = client.getDatabase(options.getString(MongoDBOptionsConf.DATABASE, null));
        this.collection = this.database
                .withReadPreference(ReadPreference.secondaryPreferred())
                .getCollection(options.getString(MongoDBOptionsConf.COLLECTION, null));
        this.collStats = collStats;
    }

    public abstract List<InputSplit> calculateSplits(int numSplits, String splitBy, Document findQuery);

    protected GenericOptions getOptions() {
        return options;
    }

    protected MongoClient getClient() {
        return client;
    }

    protected MongoDatabase getDatabase() {
        return database;
    }

    protected MongoCollection<Document> getCollection() {
        return collection;
    }

    protected Document getCollStats() {
        return collStats;
    }

    public int getCount() {
        return new BigDecimal(getCollStats().get("count").toString()).intValueExact();
    }

    /**
     * Get a list of nonempty input splits only.
     *
     * @param splits a list of input splits
     * @return a new list of nonempty input splits
     */
    protected List<InputSplit> filterEmptySplits(
            final List<InputSplit> splits, final Document findQuery) {
        return splits.stream().filter(item -> {
            ArrayList<Document> filterList = new ArrayList<>(2);
            filterList.add(((MongoDBInputSplit) item).getSplitQuery());
            if (findQuery != null && findQuery.size() > 0) {
                filterList.add(findQuery);
            }
            Document filter = filterList.size() == 1 ? filterList.get(0) : new Document("$and", filterList);
            LOG.info("Check the split emptyness: " + filter);
            return !MongoDBUtils.isEmpty(collection, filter);
        }).collect(Collectors.toList());
    }

    protected MongoDBInputSplit createSplitFromBounds(Object min, Object max, String splitBy) {
        LOG.info("Created split: min = " + (min != null ? min.toString() : "null") + ", max = " + (max != null
                ? max.toString()
                : "null"));
        // Objects to contain upper/lower bounds for each split
        min = Objects.equal(min, MIN_KEY_TYPE) ? null : min;
        max = Objects.equal(max, MAX_KEY_TYPE) ? null : max;
        return new MongoDBInputSplit(min, max, splitBy);
    }
}
