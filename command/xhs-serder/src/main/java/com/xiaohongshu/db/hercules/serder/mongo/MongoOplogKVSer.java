package com.xiaohongshu.db.hercules.serder.mongo;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BytesWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.mongodb.mr.output.MongoDBWrapperSetterManager;
import com.xiaohongshu.db.xlog.core.codec.Codec;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;
import com.xiaohongshu.db.xlog.oplog.OperatorPB;
import com.xiaohongshu.db.xlog.oplog.OplogManagerPB;
import com.xiaohongshu.db.xlog.oplog.OplogSerDe;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.time.LocalDateTime;

public class MongoOplogKVSer extends KVSer<Document> implements InjectedClass {

    private static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .build();

    private MongoOplogOutputOptionConf.OplogFormat format;

    @Options(type = OptionsType.SER)
    private GenericOptions options;

    @SchemaInfo
    private Schema schema;

    public MongoOplogKVSer() {
        super(new MongoDBWrapperSetterManager());
    }

    @Override
    public void afterInject() {
        format = MongoOplogOutputOptionConf.OplogFormat.valueOfIgnoreCase(options.getString(MongoOplogOutputOptionConf.FORMAT, null));
    }

    @Override
    protected BaseWrapper<?> writeValue(HerculesWritable in) throws IOException, InterruptedException {
        OplogManagerPB.Oplog.Builder builder = OplogManagerPB.Oplog.newBuilder();
        builder.setNs(options.getString(MongoOplogOutputOptionConf.NS, ""));
        builder.setTimestamp(System.currentTimeMillis() / 1000);
        builder.setOp(OperatorPB.Op.INSERT);
        builder.setFromMigrate(false);

        if (schema.getColumnNameList().size() != 0) {
            in = new HerculesWritable(WritableUtils.copyColumn(in.getRow(), schema.getColumnNameList(), WritableUtils.FilterUnexistOption.IGNORE));
        }

        Document document;
        try {
            document = wrapperSetterFactory.writeMapWrapper(in.getRow(), new Document(), null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String docJsonString = document.toJson(JSON_WRITER_SETTINGS);
        switch (format) {
            case JSON:
                OplogRecord oplogRecord = new OplogRecord();
                oplogRecord.setDoc(docJsonString);
                oplogRecord.setEventTime(LocalDateTime.now());
                oplogRecord.setOptype("INSERT");
                return BytesWrapper.get(JSON.toJSONString(oplogRecord).getBytes());
            case DEFAULT:
                builder.setDoc(docJsonString);
                try {
                    return BytesWrapper.get(OplogSerDe.serialize(builder.build(), Codec.CODEC_OPLOG_PB01));
                } catch (SerDeException e) {
                    // 永远来不到这
                    throw new RuntimeException(e);
                }
            default:
                throw new RuntimeException("Unknown oplog format: " + format);
        }
    }
}
