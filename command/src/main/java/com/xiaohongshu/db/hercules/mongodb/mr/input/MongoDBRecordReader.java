package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.mongodb.datatype.MongoDBCustomDataTypeManager;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBDataTypeConverter;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.io.IOException;
import java.util.*;

public class MongoDBRecordReader extends HerculesRecordReader<Document> {

    private static final Log LOG = LogFactory.getLog(MongoDBRecordReader.class);

    private Long pos = 0L;
    /**
     * 用于估算进度
     */
    private Long mapAverageRowNum;

    private MongoDBManager manager;

    private MongoCursor<Document> cursor = null;
    private MongoClient client = null;
    private HerculesWritable value;

    private MongoDBDataTypeConverter converter;

    private final Document fakeDocument = new Document(WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, 0);

    public MongoDBRecordReader(TaskAttemptContext context, MongoDBManager manager) {
        this(context, MongoDBCustomDataTypeManager.INSTANCE, manager);
    }

    public MongoDBRecordReader(TaskAttemptContext context,
                               MongoDBCustomDataTypeManager typeManager, MongoDBManager manager) {
        this(context, new MongoDBDataTypeConverter(), typeManager, manager);
    }

    public MongoDBRecordReader(TaskAttemptContext context, MongoDBDataTypeConverter converter,
                               MongoDBCustomDataTypeManager typeManager, MongoDBManager manager) {
        super(context, typeManager, null);
        this.converter = converter;
        setWrapperGetterFactory(new MongoDBWrapperGetterFactory());
        this.manager = manager;
    }

    private Document makeColumnProjection(List<String> columnNameList) {
        Document res = new Document();
        // 不需要判断，无脑置0，反正如果包含的话也会置回1
        res.put(MongoDBManager.ID, 0);
        for (String columnName : columnNameList) {
            res.put(columnName, 1);
        }
        return res;
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        mapAverageRowNum = configuration.getLong(MongoDBInputFormat.AVERAGE_MAP_ROW_NUM, 0L);

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        String databaseStr = options.getSourceOptions().getString(MongoDBOptionsConf.DATABASE, null);
        String collectionStr = options.getSourceOptions().getString(MongoDBOptionsConf.COLLECTION, null);
        String query = options.getSourceOptions().getString(MongoDBInputOptionsConf.QUERY, null);

        MongoDBInputSplit mongoDBInputSplit = (MongoDBInputSplit) split;
        Document splitQuery = mongoDBInputSplit.getSplitQuery();

        try {
            client = manager.getConnection();
            MongoCollection<Document> collection = client.getDatabase(databaseStr).getCollection(collectionStr);

            Document filter = splitQuery;
            if (!StringUtils.isEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }

            FindIterable<Document> iterable = collection.find(filter);
            if (!emptyColumnNameList) {
                Document columnProjection = makeColumnProjection(columnNameList);
                iterable.projection(columnProjection);
            }
            cursor = iterable.iterator();
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    private MapWrapper documentToMapWrapper(Document document, String documentPosition)
            throws Exception {
        MapWrapper res = new MapWrapper(document.size());
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            String columnName = entry.getKey();
            // 用于columnTypeMap
            String fullColumnName = WritableUtils.concatColumn(documentPosition, columnName);
            Object columnValue = entry.getValue();
            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, converter.convertElementType(columnValue));
            res.put(columnName, getWrapperGetter(columnType).get(document, documentPosition, columnName, -1));
        }
        return res;
    }

    private ListWrapper listToListWrapper(List list) throws Exception {
        ListWrapper res = new ListWrapper(list.size());
        for (Object columnValue : list) {
            DataType columnType = converter.convertElementType(columnValue);
            fakeDocument.put(WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, columnValue);
            res.add(getWrapperGetter(columnType).get(fakeDocument, WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST, WritableUtils.FAKE_COLUMN_NAME_USED_BY_LIST, -1));
        }
        return res;
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        try {
            if (!cursor.hasNext()) {
                LOG.info(String.format("Selected %d records.", pos));
                return false;
            }

            ++pos;

            Document item = cursor.next();
            value = new HerculesWritable(documentToMapWrapper(item, null));
            return true;
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        try {
            if (!cursor.hasNext()) {
                return 1.0f;
            } else {
                if (mapAverageRowNum == 0L) {
                    return 0.0f;
                } else {
                    return Math.min(1.0f, pos.floatValue() / mapAverageRowNum.floatValue());
                }
            }
        } catch (Exception e) {
            return 1.0f;
        }
    }

    @Override
    public void innerClose() throws IOException {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception e) {
                LOG.warn("Exception closing cursor: " + ExceptionUtils.getStackTrace(e));
            }
        }
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Exception closing client: " + ExceptionUtils.getStackTrace(e));
            }
        }
    }

    private class MongoDBWrapperGetterFactory extends WrapperGetterFactory<Document> {

        @Override
        protected WrapperGetter<Document> getByteGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Byte value = row.get(columnName, Byte.class);
                    return IntegerWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getShortGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Short value = row.get(columnName, Short.class);
                    return IntegerWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getIntegerGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Integer value = row.get(columnName, Integer.class);
                    return IntegerWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getLongGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Long value = row.get(columnName, Long.class);
                    return IntegerWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getLonglongGetter() {
            return null;
        }

        @Override
        protected WrapperGetter<Document> getFloatGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Float value = row.get(columnName, Float.class);
                    return DoubleWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getDoubleGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Double value = row.get(columnName, Double.class);
                    return DoubleWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getDecimalGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Decimal128 value = row.get(columnName, Decimal128.class);
                    return DoubleWrapper.get(value.bigDecimalValue());
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getBooleanGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Boolean value = row.get(columnName, Boolean.class);
                    return BooleanWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getStringGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    String value = row.get(columnName, String.class);
                    return StringWrapper.get(value);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getDateGetter() {
            return null;
        }

        @Override
        protected WrapperGetter<Document> getTimeGetter() {
            return null;
        }

        @Override
        protected WrapperGetter<Document> getDatetimeGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Date value = row.get(columnName, Date.class);
                    return DateWrapper.get(value, BaseDataType.DATETIME);
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getBytesGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Binary value = row.get(columnName, Binary.class);
                    return BytesWrapper.get(value.getData());
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getNullGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    return NullWrapper.INSTANCE;
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getListGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    ArrayList value = row.get(columnName, ArrayList.class);
                    if (value == null) {
                        return ListWrapper.NULL_INSTANCE;
                    } else {
                        return listToListWrapper(value);
                    }
                }
            };
        }

        @Override
        protected WrapperGetter<Document> getMapGetter() {
            return new WrapperGetter<Document>() {
                @Override
                public BaseWrapper get(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                    Document value = row.get(columnName, Document.class);
                    if (value == null) {
                        return MapWrapper.NULL_INSTANCE;
                    } else {
                        String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                        return documentToMapWrapper(value, fullColumnName);
                    }
                }
            };
        }
    }
}
