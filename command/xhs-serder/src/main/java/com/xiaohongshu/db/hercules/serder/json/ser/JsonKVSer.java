package com.xiaohongshu.db.hercules.serder.json.ser;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BytesWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;

public class JsonKVSer extends KVSer<Document> {

    private static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.STRICT)
            .build();

    @Options(type = OptionsType.SER)
    private GenericOptions options;

    @SchemaInfo
    private Schema schema;

    public JsonKVSer() {
        super(new JsonWrapperSetterManager());
    }

    @Override
    protected BaseWrapper<?> writeValue(HerculesWritable in) throws IOException, InterruptedException {


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
        return BytesWrapper.get(docJsonString.getBytes());
    }
}