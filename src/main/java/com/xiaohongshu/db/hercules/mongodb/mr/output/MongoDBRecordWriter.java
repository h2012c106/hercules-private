package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.MultiThreadAsyncWriter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.mongodb.ExportType;
import com.xiaohongshu.db.hercules.mongodb.datatype.MongoDBCustomDataTypeManager;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOutputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST;
import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST;
import static com.xiaohongshu.db.hercules.mongodb.option.MongoDBOutputOptionsConf.DECIMAL_AS_STRING;

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
    private final boolean decimalAsString;

    private final MongoDBMultiThreadAsyncWriter writer;

    public MongoDBRecordWriter(TaskAttemptContext context, MongoDBManager manager, MongoDBCustomDataTypeManager typeManager) throws Exception {
        super(context, null, typeManager);
        setWrapperSetterFactory(new MongoDBWrapperSetterFactory());

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

        decimalAsString = options.getTargetOptions().getBoolean(DECIMAL_AS_STRING, false);

        recordList = new ArrayList<>(statementPerBulk.intValue());

        int threadNum = options.getTargetOptions().getInteger(MongoDBOutputOptionsConf.EXECUTE_THREAD_NUM, null);
        writer = new MongoDBMultiThreadAsyncWriter(threadNum);
        writer.run();
    }

    /**
     * 当上游不是list时，做一个singleton list
     *
     * @param wrapper
     * @return
     * @throws Exception
     */
    private ArrayList<Object> wrapperToList(BaseWrapper wrapper) throws Exception {
        ArrayList<Object> res;
        if (wrapper.getType() == BaseDataType.LIST) {
            ListWrapper listWrapper = (ListWrapper) wrapper;
            res = new ArrayList<>(listWrapper.size());
            for (int i = 0; i < listWrapper.size(); ++i) {
                BaseWrapper subWrapper = listWrapper.get(i);
                // 由于不能指定list下的类型故只能抄作业
                DataType dataType = subWrapper.getType();
                // 不能像reader一样共享一个，写会有多线程情况，要慎重
                Document tmpDocument = new Document();
                getWrapperSetter(dataType).set(subWrapper, tmpDocument, FAKE_PARENT_NAME_USED_BY_LIST, FAKE_COLUMN_NAME_USED_BY_LIST, -1);
                Object convertedValue = tmpDocument.get(FAKE_COLUMN_NAME_USED_BY_LIST);
                res.add(convertedValue);
            }
        } else {
            DataType dataType = wrapper.getType();
            Document tmpDocument = new Document();
            getWrapperSetter(dataType).set(wrapper, tmpDocument, FAKE_PARENT_NAME_USED_BY_LIST, FAKE_COLUMN_NAME_USED_BY_LIST, -1);
            Object convertedValue = tmpDocument.get(FAKE_COLUMN_NAME_USED_BY_LIST);
            res = Lists.newArrayList(convertedValue);
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
            getWrapperSetter(columnType).set(subWrapper, res, columnPath, columnName, -1);
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

    private class MongoDBWrapperSetterFactory extends WrapperSetterFactory<Document> {

        @Override
        protected WrapperSetter<Document> getByteSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
                    if (res == null) {
                        row.put(columnName, null);
                    } else {
                        row.put(columnName, res.byteValueExact());
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getShortSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
                    if (res == null) {
                        row.put(columnName, null);
                    } else {
                        row.put(columnName, res.shortValueExact());
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getIntegerSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
                    if (res == null) {
                        row.put(columnName, null);
                    } else {
                        row.put(columnName, res.intValueExact());
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getLongSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
                    if (res == null) {
                        row.put(columnName, null);
                    } else {
                        row.put(columnName, res.longValueExact());
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getLonglongSetter() {
            return null;
        }

        @Override
        protected WrapperSetter<Document> getFloatSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    BigDecimal res = wrapper.asBigDecimal();
                    // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
                    if (res == null) {
                        row.put(columnName, null);
                    } else {
                        row.put(columnName, OverflowUtils.numberToFloat(res));
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getDoubleSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    BigDecimal res = wrapper.asBigDecimal();
                    // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
                    if (res == null) {
                        row.put(columnName, null);
                    } else {
                        row.put(columnName, OverflowUtils.numberToDouble(res));
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getDecimalSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    BigDecimal res = wrapper.asBigDecimal();
                    // 由于能走到这一步的一定存在对应列名，故不用纠结上游为null时需要无视还是置null
                    if (res == null) {
                        row.put(columnName, null);
                    } else {
                        if (decimalAsString) {
                            row.put(columnName, res.toPlainString());
                        } else {
                            row.put(columnName, new Decimal128(res));
                        }
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getBooleanSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    row.put(columnName, wrapper.asBoolean());
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getStringSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    row.put(columnName, wrapper.asString());
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getDateSetter() {
            return null;
        }

        @Override
        protected WrapperSetter<Document> getTimeSetter() {
            return null;
        }

        @Override
        protected WrapperSetter<Document> getDatetimeSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    row.put(columnName, wrapper.asDate());
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getBytesSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    row.put(columnName, new Binary(wrapper.asBytes()));
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getNullSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    row.put(columnName, null);
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getListSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    if (wrapper.isNull()) {
                        row.put(columnName, null);
                    } else {
                        row.put(columnName, wrapperToList(wrapper));
                    }
                }
            };
        }

        @Override
        protected WrapperSetter<Document> getMapSetter() {
            return new WrapperSetter<Document>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, Document row, String rowName, String columnName, int columnSeq)
                        throws Exception {
                    if (wrapper.isNull()) {
                        // 有可能是个null，不处理else要NPE
                        row.put(columnName, null);
                    } else if (wrapper.getType() == BaseDataType.MAP) {
                        // 确保尽可能不丢失类型信息
                        String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                        row.put(columnName, mapWrapperToDocument((MapWrapper) wrapper, fullColumnName));
                    } else {
                        row.put(columnName, ((JSONObject) wrapper.asJson()).getInnerMap());
                    }
                }
            };
        }
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
