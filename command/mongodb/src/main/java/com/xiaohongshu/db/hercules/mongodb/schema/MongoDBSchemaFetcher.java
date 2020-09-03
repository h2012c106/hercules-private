package com.xiaohongshu.db.hercules.mongodb.schema;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.mongodb.MongoDBUtils;
import org.bson.Document;

import java.util.*;

import static com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf.COLLECTION;
import static com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf.DATABASE;

public class MongoDBSchemaFetcher extends BaseSchemaFetcher {

    public MongoDBSchemaFetcher(GenericOptions options) {
        super(options);
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        return null;
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap() {
        return null;
    }

    @Override
    protected List<Set<String>> innerGetIndexGroupList() {
        List<Set<String>> res = new LinkedList<>();
        res.add(Sets.newHashSet(MongoDBUtils.ID));
        for (Document indexInfo : MongoDBUtils.getConnection(getOptions())
                .getDatabase(getOptions().getString(DATABASE, null))
                .getCollection(getOptions().getString(COLLECTION, null))
                .listIndexes()) {
            List<String> indexList = new ArrayList<>(indexInfo.get("key", Document.class).keySet());
            for (Set<String> unwrappedIndex : SchemaUtils.unwrapIndexList(indexList)) {
                if (!res.contains(unwrappedIndex)) {
                    res.add(unwrappedIndex);
                }
            }
        }
        return res;
    }

    @Override
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        List<Set<String>> res = new LinkedList<>();
        res.add(Sets.newHashSet(MongoDBUtils.ID));
        for (Document indexInfo : MongoDBUtils.getConnection(getOptions())
                .getDatabase(getOptions().getString(DATABASE, null))
                .getCollection(getOptions().getString(COLLECTION, null))
                .listIndexes()) {
            if (indexInfo.getBoolean("unique", false)) {
                Set<String> ukSet = indexInfo.get("key", Document.class).keySet();
                if (!res.contains(ukSet)) {
                    res.add(ukSet);
                }
            }
        }
        return res;
    }

}
