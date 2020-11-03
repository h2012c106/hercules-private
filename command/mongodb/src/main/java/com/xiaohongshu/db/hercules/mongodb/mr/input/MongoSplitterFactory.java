package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoDatabase;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.MongoSplitter;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.SampleSplitter;
import com.xiaohongshu.db.hercules.mongodb.mr.input.splitter.StandaloneMongoSplitter;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import java.util.List;

/**
 * 所有相关代码核心逻辑抄自 https://github.com/mongodb/mongo-hadoop/blob/master/core/src/main/java/com/mongodb/hadoop/splitter
 */
public final class MongoSplitterFactory {

    private static final Log LOG = LogFactory.getLog(MongoSplitterFactory.class);

    private static final int MONGO_UNAUTHORIZED_ERR_CODE = 13;
    private static final int MONGO_ILLEGALOP_ERR_CODE = 20;

    public static MongoSplitter getSplitter(final MongoClient client, GenericOptions options) {
        /* Looks at the collection in mongo.input.uri
         * and choose an implementation based on what's in there.  */

        MongoSplitter returnVal;

        String splitBy = options.getString(MongoDBInputOptionsConf.SPLIT_BY, null);
        String databaseStr = options.getString(MongoDBOptionsConf.DATABASE, null);
        String collectionStr = options.getString(MongoDBOptionsConf.COLLECTION, null);

        MongoDatabase database = client.getDatabase(databaseStr);
        Document stats = database.runCommand(new Document("collStats", collectionStr));
        Document buildInfo = database.runCommand(new Document("buildinfo", ""));

        if (!stats.getBoolean("ok", false)) {
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

            if (sampleOperatorSupported) {
                LOG.info("Unsharded mongo, use `SampleSplitter`.");
                returnVal = new SampleSplitter(options, client, stats);
            } else if (supportSplitVector) {
                LOG.info("Unsharded mongo, use `StandaloneMongoSplitter`.");
                returnVal = new StandaloneMongoSplitter(options, client, stats);
            } else {
                throw new UnsupportedOperationException("Not define the `SkipSplitter` yet.");
            }
        } else {
            if (supportSplitVector) {
                LOG.info("Sharded mongo, use `StandaloneMongoSplitter`.");
                returnVal = new StandaloneMongoSplitter(options, client, stats);
            } else {
                throw new UnsupportedOperationException("Not define the `SkipSplitter` yet.");
            }
        }

        return returnVal;
    }

}
