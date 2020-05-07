package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.*;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.mongodb.mr.DocumentWithColumnPath;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBDataTypeConverter;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongoDBRecordReader
        extends HerculesRecordReader<DocumentWithColumnPath, MongoDBDataTypeConverter> {

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

    private AtomicBoolean hasClosed = new AtomicBoolean(false);

    /**
     * List->ListWrapper企图复用WrapperGetter的无奈之举，不然需要写一大长串DataType的switch case去Object转Wrapper
     */
    private final String fakeColumnName = "FAKE_NAME";
    private final Document fakeDocument = new Document(fakeColumnName, 0);
    private final DocumentWithColumnPath fakeDocumentWithColumnPath = new DocumentWithColumnPath(fakeDocument, "###LIST###");

    public MongoDBRecordReader(MongoDBDataTypeConverter converter, MongoDBManager manager) {
        super(converter);
        this.manager = manager;
    }

    private Document makeColumnProjection(List<String> columnNameList) {
        Document res = new Document();
        if (!columnNameList.contains(MongoDBManager.ID)) {
            res.put(MongoDBManager.ID, 0);
        }
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

    private MapWrapper documentToMapWrapper(DocumentWithColumnPath documentWithColumnPath)
            throws Exception {
        Document document = documentWithColumnPath.getDocument();
        String columnPath = documentWithColumnPath.getColumnPath();
        MapWrapper res = new MapWrapper(document.size());
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            String columnName = entry.getKey();
            // 用于columnTypeMap
            String fullColumnName = WritableUtils.concatColumn(columnPath, columnName);
            Object columnValue = entry.getValue();
            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, converter.convertElementType(columnValue));
            res.put(columnName, getWrapperGetter(columnType)
                    .get(documentWithColumnPath, columnName, -1));
        }
        return res;
    }

    private ListWrapper listToListWrapper(List list) throws Exception {
        ListWrapper res = new ListWrapper(list.size());
        for (Object columnValue : list) {
            DataType columnType = converter.convertElementType(columnValue);
            fakeDocument.put(fakeColumnName, columnValue);
            res.add(getWrapperGetter(columnType).get(fakeDocumentWithColumnPath, fakeColumnName, -1));
        }
        return res;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (!cursor.hasNext()) {
                LOG.info(String.format("Selected %d records.", pos));
                return false;
            }

            ++pos;

            Document item = cursor.next();
            value = new HerculesWritable(documentToMapWrapper(new DocumentWithColumnPath(item, null)));
            return true;
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public HerculesWritable getCurrentValue() throws IOException, InterruptedException {
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
    public void close() throws IOException {
        if (!hasClosed.getAndSet(true)) {
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
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getIntegerGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                Object value = row.getDocument().get(name);
                if (value == null) {
                    return NullWrapper.INSTANCE;
                } else if (value instanceof Integer) {
                    return new IntegerWrapper((Integer) value);
                } else if (value instanceof Long) {
                    return new IntegerWrapper((Long) value);
                } else {
                    return new IntegerWrapper(NumberUtils.createBigInteger(value.toString()));
                }
            }
        };
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getDoubleGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                Object value = row.getDocument().get(name);
                if (value == null) {
                    return NullWrapper.INSTANCE;
                } else if (value instanceof Float) {
                    return new DoubleWrapper((Float) value);
                } else if (value instanceof Double) {
                    return new DoubleWrapper((Double) value);
                } else {
                    return new DoubleWrapper(NumberUtils.createBigDecimal(value.toString()));
                }
            }
        };
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getBooleanGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                Object value = row.getDocument().get(name);
                if (value == null) {
                    return NullWrapper.INSTANCE;
                } else {
                    return new BooleanWrapper((Boolean) value);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getStringGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                Object value = row.getDocument().get(name);
                if (value == null) {
                    return NullWrapper.INSTANCE;
                } else {
                    return new StringWrapper(value.toString());
                }
            }
        };
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getDateGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                Object value = row.getDocument().get(name);
                if (value == null) {
                    return NullWrapper.INSTANCE;
                } else {
                    return new DateWrapper((Date) value);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getBytesGetter() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getNullGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                return NullWrapper.INSTANCE;
            }
        };
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getListGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                Object value = row.getDocument().get(name);
                if (value == null) {
                    return NullWrapper.INSTANCE;
                } else {
                    return listToListWrapper((ArrayList) value);
                }
            }
        };
    }

    @Override
    protected WrapperGetter<DocumentWithColumnPath> getMapGetter() {
        return new WrapperGetter<DocumentWithColumnPath>() {
            @Override
            public BaseWrapper get(DocumentWithColumnPath row, String name, int seq) throws Exception {
                Object value = row.getDocument().get(name);
                if (value == null) {
                    return NullWrapper.INSTANCE;
                } else {
                    String fullColumnName = WritableUtils.concatColumn(row.getColumnPath(), name);
                    return documentToMapWrapper(new DocumentWithColumnPath((Document) value, fullColumnName));
                }
            }
        };
    }
}
