package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.xlog.core.codec.Codec;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;
import com.xiaohongshu.db.xlog.oplog.OperatorPB;
import com.xiaohongshu.db.xlog.oplog.OplogManagerPB;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.xlog.oplog.OplogSerDe;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MongoOplogKvConverter extends KvConverter<Integer, Integer, Document> {

    private static final JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
//        .dateTimeConverter((time, writer)->{
//            writer.writeStartObject();
//            writer.writeString("$date", String.valueOf(time));
//            writer.writeEndObject();
//        })
            .build();

    public MongoOplogKvConverter(GenericOptions options) {
        super(null, new MongoOplogWrapperGetterFactory(), new MongoOplogWrapperSetterFactory(), options);
        MongoOplogWrapperSetterFactory mongoOplogWrapperSetterFactory = (MongoOplogWrapperSetterFactory) this.wrapperSetterFactory;
        Map<String, DataType> columnTypeMap = SchemaUtils.convert(options.getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, new com.alibaba.fastjson.JSONObject()), NullCustomDataTypeManager.INSTANCE);
        mongoOplogWrapperSetterFactory.setColumnTypeMap(columnTypeMap);
    }

    @Override
    public byte[] generateValue(HerculesWritable value, GenericOptions options, Map<String, DataType> columnTypeMap, List<String> columnNameList) {

        OplogManagerPB.Oplog.Builder builder = OplogManagerPB.Oplog.newBuilder();
        builder.setNs(options.getString(MongoOplogOutputOptionConf.NS, ""));
        // TODO 后续增加用设置column取ts的逻辑
        builder.setTimestamp(System.currentTimeMillis() / 1000);
        builder.setOp(OperatorPB.Op.INSERT);
        builder.setFromMigrate(false);
        Document doc = new Document();
        if (columnNameList.size() == 0) {
            for (Map.Entry<String, BaseWrapper> entry : value.entrySet()) {

                BaseWrapper wrapper = entry.getValue();
                String columnName = entry.getKey();
                DataType type = columnTypeMap.get(columnName);
                if (type == null) {
                    type = wrapper.getType();
                }
                constructDoc(doc, columnName, wrapper, type);
            }
        } else {
            for (String columnName : columnNameList) {

                BaseWrapper wrapper = value.get(columnName);
                if (wrapper == null) {
                    continue;
                }
                DataType type = columnTypeMap.get(columnName);
                if (type == null) {
                    type = wrapper.getType();
                }
                constructDoc(doc, columnName, wrapper, type);
            }
        }
        builder.setDoc(doc.toJson(jsonWriterSettings));
        try {
            return OplogSerDe.serialize(builder.build(), Codec.CODEC_OPLOG_PB01);
        } catch (SerDeException ignored) {
        }
        return null;
    }

    private void constructDoc(Document doc, String columnName, BaseWrapper wrapper, DataType type) {

        try {
            getWrapperSetter(type).set(wrapper, doc, "", columnName, 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HerculesWritable generateHerculesWritable(byte[] data, GenericOptions options) throws IOException {
        return null;
    }
}
