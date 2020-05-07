package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.MultiThreadAsyncWriter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.serialize.datatype.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.datatype.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.mongodb.ExportType;
import com.xiaohongshu.db.hercules.mongodb.mr.DocumentWithColumnPath;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOutputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MongoDBRecordWriter extends HerculesRecordWriter<DocumentWithColumnPath> {

    private static final Log LOG = LogFactory.getLog(MongoDBRecordWriter.class);

    private MongoClient client;
    private List<HerculesWritable> recordList;

    private final String dbName;
    private final String collectionName;

    private final Set<String> objectIdColumnNameSet;
    private final ExportType exportType;
    private final boolean upsert;
    private final Long statementPerBulk;
    private final List<String> updateKeyList;
    private final boolean bulkOrdered;

    private final MongoDBMultiThreadAsyncWriter writer;

    /**
     * List->ListWrapper企图复用WrapperSetter的无奈之举，不然需要写一大长串DataType的switch case去Object转Wrapper
     */
    private final String fakeColumnName = "FAKE_NAME";

    public MongoDBRecordWriter(TaskAttemptContext context, MongoDBManager manager) throws Exception {
        super(context);
        client = manager.getConnection();

        dbName = options.getTargetOptions().getString(MongoDBOptionsConf.DATABASE, null);
        collectionName = options.getTargetOptions().getString(MongoDBOptionsConf.COLLECTION, null);

        objectIdColumnNameSet = Arrays
                .stream(options.getTargetOptions().getStringArray(MongoDBOutputOptionsConf.OBJECT_ID, null))
                .collect(Collectors.toSet());
        // 保证objectId列为String型
        for (String objectIdName : objectIdColumnNameSet) {
            // objectId的列要么不定义，定义了必须是String
            if (columnTypeMap.containsKey(objectIdName) && columnTypeMap.get(objectIdName) != DataType.STRING) {
                LOG.warn(String.format("Unexpected column type [%s] of a objectId column: %s. Force to STRING.",
                        columnTypeMap.get(objectIdName), objectIdName));
            }
            columnTypeMap.put(objectIdName, DataType.STRING);
        }

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

    private ArrayList<Object> listWrapperToList(ListWrapper wrapper) throws Exception {
        ArrayList<Object> res = new ArrayList<>(wrapper.size());
        for (int i = 0; i < wrapper.size(); ++i) {
            BaseWrapper subWrapper = wrapper.get(i);
            // 由于不能指定list下的类型故只能抄作业
            DataType dataType = subWrapper.getType();
            // 不能像reader一样共享一个，写会有多线程情况，要慎重
            DocumentWithColumnPath tmpDocumentWithColumnPath = new DocumentWithColumnPath(new Document(), null);
            getWrapperSetter(dataType).set(subWrapper, tmpDocumentWithColumnPath, fakeColumnName, -1);
            Object convertedValue = tmpDocumentWithColumnPath.getDocument().get(fakeColumnName);
            res.add(convertedValue);
        }
        return res;
    }

    @SneakyThrows
    private Document mapWrapperToDocument(MapWrapper wrapper, String columnPath) {
        Document res = new Document();
        for (Map.Entry<String, BaseWrapper> entry : wrapper.entrySet()) {
            String columnName = entry.getKey();
            String fullColumnName = WritableUtils.concatColumn(columnPath, columnName);
            BaseWrapper subWrapper = entry.getValue();
            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, subWrapper.getType());
            getWrapperSetter(columnType).set(subWrapper, new DocumentWithColumnPath(res, columnPath), columnName, -1);
        }
        return res;
    }

    private void execUpdate() throws IOException, InterruptedException {
        if (recordList.size() <= 0) {
            return;
        }

        List<WriteModel<Document>> writeModelList = new ArrayList<>(recordList.size());
        if (exportType.isInsert()) {
            writeModelList.addAll(recordList.stream()
                    .map(record -> mapWrapperToDocument(record.getRow(), null))
                    .map(InsertOneModel::new)
                    .collect(Collectors.toList())
            );
        } else {
            for (HerculesWritable record : recordList) {
                Document value = mapWrapperToDocument(record.getRow(), null);
                Document filter = mapWrapperToDocument(WritableUtils.copyColumn(record.getRow(),
                        updateKeyList, WritableUtils.FilterUnexistOption.THROW), null);
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

    /**
     * 把相关列过滤出来，然后用{@link #innerMapWrite(HerculesWritable)}
     *
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException {
        value = new HerculesWritable(WritableUtils.copyColumn(value.getRow(), columnNameList, WritableUtils.FilterUnexistOption.IGNORE));
        innerMapWrite(value);
    }

    @Override
    protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException {
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
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getIntegerSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                document.put(name, wrapper.asBigInteger().longValue());
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getDoubleSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                document.put(name, wrapper.asDouble());
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getBooleanSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                document.put(name, wrapper.asBoolean());
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getStringSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                String fullColumnName = WritableUtils.concatColumn(row.getColumnPath(), name);
                // 配置为ObjectId的列，必须显式指定为String型
                if (objectIdColumnNameSet.contains(fullColumnName)) {
                    document.put(name, new ObjectId(wrapper.asString()));
                } else {
                    document.put(name, wrapper.asString());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getDateSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                document.put(name, wrapper.asDate());
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getBytesSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                document.put(name, wrapper.asBytes());
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getNullSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                document.put(name, null);
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getListSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                // 确保尽可能不丢失类型信息
                if (wrapper instanceof ListWrapper) {
                    document.put(name, listWrapperToList((ListWrapper) wrapper));
                } else {
                    document.put(name, ((JSONArray) wrapper.asJson()).toJavaList(Object.class));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<DocumentWithColumnPath> getMapSetter() {
        return new WrapperSetter<DocumentWithColumnPath>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, DocumentWithColumnPath row, String name, int seq) throws Exception {
                Document document = row.getDocument();
                // 确保尽可能不丢失类型信息
                if (wrapper instanceof MapWrapper) {
                    String fullColumnName = WritableUtils.concatColumn(row.getColumnPath(), name);
                    document.put(name, mapWrapperToDocument((MapWrapper) wrapper, fullColumnName));
                } else {
                    document.put(name, ((JSONObject) wrapper.asJson()).getInnerMap());
                }
            }
        };
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
