package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.MultiThreadAsyncWriter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.mongodb.ExportType;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOutputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MongoDBRecordWriter extends HerculesRecordWriter<Document> {

    private static final Log LOG = LogFactory.getLog(MongoDBRecordWriter.class);

    private MongoClient client;
    private List<HerculesWritable> recordList;

    private final String dbName;
    private final String collectionName;

    private final ExportType exportType;
    private final boolean upsert;
    private final Long statementPerBulk;
    private final List<String> updateKeyList;
    private final boolean bulkOrdered;

    private final MongoDBMultiThreadAsyncWriter writer;

    public MongoDBRecordWriter(TaskAttemptContext context, MongoDBManager manager) throws Exception {
        super(context);

        client = manager.getConnection();

        dbName = options.getTargetOptions().getString(MongoDBOptionsConf.DATABASE, null);
        collectionName = options.getTargetOptions().getString(MongoDBOptionsConf.COLLECTION, null);

        exportType = ExportType.valueOfIgnoreCase(options.getTargetOptions().getString(MongoDBOutputOptionsConf.EXPORT_TYPE, null));
        upsert = options.getTargetOptions().getBoolean(MongoDBOutputOptionsConf.UPSERT, false);
        statementPerBulk = options.getTargetOptions().getLong(MongoDBOutputOptionsConf.STATEMENT_PER_BULK, null);
        if (!exportType.isInsert()) {
            updateKeyList = Arrays.asList(options.getTargetOptions().getStringArray(MongoDBOutputOptionsConf.UPDATE_KEY, null));
            if (updateKeyList.size() == 0) {
                throw new RuntimeException(String.format("Zero-length update key list when using %s mode.", exportType.name()));
            }
        } else {
            updateKeyList = null;
        }
        bulkOrdered = options.getTargetOptions().getBoolean(MongoDBOutputOptionsConf.BULK_ORDERED, false);

        recordList = new ArrayList<>(statementPerBulk.intValue());

        int threadNum = options.getTargetOptions().getInteger(MongoDBOutputOptionsConf.EXECUTE_THREAD_NUM, null);
        writer = new MongoDBMultiThreadAsyncWriter(threadNum);
        writer.run();
    }

    @Override
    protected void afterSetWrapperSetterFactory() {
        boolean decimalAsString = options.getTargetOptions().getBoolean(MongoDBOutputOptionsConf.DECIMAL_AS_STRING, false);
        String mongoServerVersionStr = client.getDatabase(dbName)
                .runCommand(new Document("buildinfo", ""))
                .get("version")
                .toString()
                .trim();
        List<Integer> mongoServerVersion = Arrays.stream(mongoServerVersionStr.split("\\."))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        if (mongoServerVersion.get(0) < 3 || mongoServerVersion.get(1) < 4) {
            LOG.warn(String.format("The mongo version [%s] doesn't support decimal, force to string.", mongoServerVersionStr));
            decimalAsString = true;
        }
        ((MongoDBWrapperSetterManager) wrapperSetterFactory).setDecimalAsString(decimalAsString);
    }

    private void execUpdate() throws IOException, InterruptedException {
        if (recordList.size() <= 0) {
            return;
        }

        List<WriteModel<Document>> writeModelList = new ArrayList<>(recordList.size());
        if (exportType.isInsert()) {
            writeModelList.addAll(recordList.stream()
                    .map(new Function<HerculesWritable, Document>() {
                        @SneakyThrows
                        @Override
                        public Document apply(HerculesWritable record) {
                            return wrapperSetterFactory.writeMapWrapper(record.getRow(), new Document(), null);
                        }
                    })
                    .map(InsertOneModel::new)
                    .collect(Collectors.toList())
            );
        } else {
            for (HerculesWritable record : recordList) {
                Document value = null;
                Document filter = null;
                try {
                    value = wrapperSetterFactory.writeMapWrapper(record.getRow(), new Document(), null);
                    filter = wrapperSetterFactory.writeMapWrapper(WritableUtils.copyColumn(record.getRow(),
                            updateKeyList, WritableUtils.FilterUnexistOption.EXCEPTION), new Document(), null);
                } catch (Exception e) {
                    throw new IOException(e);
                }
                // 即update
                if (!exportType.isReplaceOne()) {
                    // 第一层的列是update，第二层后的细节会被变成replace
                    value = new Document("$set", value);
                }
                UpdateOptions updateOptions = new UpdateOptions().upsert(upsert);
                WriteModel<Document> tmpWriteModel;
                switch (exportType) {
                    case UPDATE_ONE:
                        tmpWriteModel = new UpdateOneModel<>(filter, value, updateOptions);
                        break;
                    case UPDATE_MANY:
                        tmpWriteModel = new UpdateManyModel<>(filter, value, updateOptions);
                        break;
                    case REPLACE_ONE:
                        tmpWriteModel = new ReplaceOneModel<>(filter, value, updateOptions);
                        break;
                    default:
                        throw new RuntimeException("Unknown export type: " + exportType.name());
                }
                writeModelList.add(tmpWriteModel);
            }
        }
        recordList.clear();

        // 把writeModelList塞到自定义的Mission当中，并塞入writer
        writer.put(new MongoDBWorkerMission(false, writeModelList));
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.IGNORE;
    }

    @Override
    protected void innerWrite(HerculesWritable value) throws IOException, InterruptedException {
        recordList.add(value);
        if (recordList.size() >= statementPerBulk) {
            execUpdate();
        }
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        // 把没凑满的缓存内容全部flush掉
        execUpdate();

        writer.done();

        client.close();
    }

    public class MongoDBMultiThreadAsyncWriter extends MultiThreadAsyncWriter<MongoDBMultiThreadAsyncWriter.ThreadContext, MongoDBWorkerMission> {

        public MongoDBMultiThreadAsyncWriter(int threadNum) {
            super(threadNum);
        }

        @Override
        protected ThreadContext initializeThreadContext() throws Exception {
            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
            return new ThreadContext(collection);
        }

        @Override
        protected void doWrite(ThreadContext context, MongoDBWorkerMission mission) throws Exception {
            if (mission.getWriteModelList() != null && mission.getWriteModelList().size() > 0) {
                context.addRecordNum(mission.getWriteModelList().size());

                BulkWriteResult writeResult = context.getCollection()
                        .bulkWrite(mission.getWriteModelList(), new BulkWriteOptions().ordered(bulkOrdered));

                context.addMatchedNum(writeResult.getMatchedCount() + writeResult.getInsertedCount());
                context.addAffectedNum(writeResult.getModifiedCount() + writeResult.getInsertedCount());
                context.increaseExecuteNum();
            }
        }

        @Override
        protected void handleException(ThreadContext context, MongoDBWorkerMission mission, Exception e) {
            context.increaseErrorNum();
        }

        @Override
        protected void closeContext(ThreadContext context) {
            LOG.info(String.format("Thread %s done with %d errors, execute %d records in %d executes, match %d row, affect %d row.",
                    Thread.currentThread().getName(),
                    context.getErrorNum(),
                    context.getRecordNum(),
                    context.getExecuteNum(),
                    context.getMatchedNum(),
                    context.getAffectedNum()));
        }

        @Override
        protected MongoDBWorkerMission innerGetCloseMission() {
            return new MongoDBWorkerMission(true, null);
        }

        private class ThreadContext {
            private long matchedNum = 0L;
            private long affectedNum = 0L;
            private long recordNum = 0L;
            private long executeNum = 0L;
            private long errorNum = 0L;
            private MongoCollection<Document> collection;

            public ThreadContext(MongoCollection<Document> collection) {
                this.collection = collection;
            }

            public MongoCollection<Document> getCollection() {
                return collection;
            }

            public long getMatchedNum() {
                return matchedNum;
            }

            public long getAffectedNum() {
                return affectedNum;
            }

            public long getRecordNum() {
                return recordNum;
            }

            public long getExecuteNum() {
                return executeNum;
            }

            public long getErrorNum() {
                return errorNum;
            }

            public void addMatchedNum(long matchedNum) {
                this.matchedNum += matchedNum;
            }

            public void addAffectedNum(long affectedNum) {
                this.affectedNum += affectedNum;
            }

            public void addRecordNum(long recordNum) {
                this.recordNum += recordNum;
            }

            public void increaseExecuteNum() {
                ++executeNum;
            }

            public void increaseErrorNum() {
                ++errorNum;
            }

        }
    }

    public static class MongoDBWorkerMission extends MultiThreadAsyncWriter.WorkerMission {
        private List<WriteModel<Document>> writeModelList;

        public MongoDBWorkerMission(boolean close, List<WriteModel<Document>> writeModelList) {
            super(close);
            this.writeModelList = writeModelList;
        }

        public List<WriteModel<Document>> getWriteModelList() {
            return writeModelList;
        }
    }
}
