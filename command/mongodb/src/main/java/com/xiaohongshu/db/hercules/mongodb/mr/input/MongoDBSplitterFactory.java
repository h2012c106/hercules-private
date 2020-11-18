package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoDatabase;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.MongoDBSplitter;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.SampleMongoDBSplitter;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.SkipMongoDBSplitter;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.StandaloneMongoDBSplitter;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import java.util.List;

public final class MongoDBSplitterFactory {

    private static final Log LOG = LogFactory.getLog(MongoDBSplitterFactory.class);

    private static final int MONGO_UNAUTHORIZED_ERR_CODE = 13;
    private static final int MONGO_ILLEGALOP_ERR_CODE = 20;

    public static MongoDBSplitter getSplitter(final MongoClient client, GenericOptions options) {
        /* Looks at the collection in mongo.input.uri
         * and choose an implementation based on what's in there.  */

        MongoDBSplitter returnVal;

        String splitBy = options.getString(MongoDBInputOptionsConf.SPLIT_BY, null);
        String databaseStr = options.getString(MongoDBOptionsConf.DATABASE, null);
        String collectionStr = options.getString(MongoDBOptionsConf.COLLECTION, null);

        MongoDatabase database = client.getDatabase(databaseStr);
        Document stats = database.runCommand(new Document("collStats", collectionStr));
        Document buildInfo = database.runCommand(new Document("buildinfo", ""));

        if (stats.getDouble("ok") == 0.0) {
            throw new RuntimeException("Unable to calculate input splits from collection stats: " + stats.getString("errmsg"));
        }

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

        if (!stats.getBoolean("sharded", false)) {
            // Prefer SampleSplitter.
            List versionArray = (List) buildInfo.get("versionArray");
            boolean sampleOperatorSupported = (
                    (Integer) versionArray.get(0) > 3
                            || ((Integer) versionArray.get(0) == 3
                            && (Integer) versionArray.get(1) >= 2));
            String versionStr = buildInfo.getString("version");
            versionStr = versionStr == null ? "UNKNOWN" : versionStr.trim();
            LOG.info("Mongo version is: " + versionStr);

            if (sampleOperatorSupported) {
                LOG.info("Unsharded mongo, use `SampleMongoDBSplitter`.");
                returnVal = new SampleMongoDBSplitter(options, client, stats);
            } else if (supportSplitVector) {
                LOG.info("Unsharded mongo, use `StandaloneMongoDBSplitter`.");
                returnVal = new StandaloneMongoDBSplitter(options, client, stats);
            } else {
                LOG.info("Unsharded mongo, use `SkipMongoDBSplitter`.");
                returnVal = new SkipMongoDBSplitter(options, client, stats);
            }
        } else {
            if (supportSplitVector) {
                LOG.info("Sharded mongo, use `StandaloneMongoDBSplitter`.");
                returnVal = new StandaloneMongoDBSplitter(options, client, stats);
            } else {
                LOG.info("Sharded mongo, use `SkipMongoDBSplitter`.");
                returnVal = new SkipMongoDBSplitter(options, client, stats);
            }
        }

        return returnVal;
    }

}
