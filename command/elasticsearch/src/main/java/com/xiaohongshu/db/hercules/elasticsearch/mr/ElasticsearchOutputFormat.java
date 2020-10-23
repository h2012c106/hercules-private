package com.xiaohongshu.db.hercules.elasticsearch.mr;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.elasticsearch.option.ElasticsearchOptionConf;
import com.xiaohongshu.db.hercules.elasticsearch.option.ElasticsearchOutputOptionConf;
import com.xiaohongshu.db.hercules.elasticsearch.schema.manager.DocRequest;
import com.xiaohongshu.db.hercules.elasticsearch.schema.manager.ElasticsearchManager;
import com.xiaohongshu.db.hercules.serder.json.ser.JsonKVSer;
import com.xiaohongshu.db.hercules.serder.json.ser.JsonWrapperSetterManager;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ElasticsearchOutputFormat extends HerculesOutputFormat<Document> {

    @Options(type = OptionsType.TARGET)
    private GenericOptions options;

    @Override
    protected HerculesRecordWriter<Document> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        ElasticsearchManager manager = new ElasticsearchManager(options.getString(ElasticsearchOptionConf.ENDPOINT, ""),
                options.getInteger(ElasticsearchOptionConf.PORT, 0), options.getString(ElasticsearchOptionConf.DOCUMENT_TYPE, "doc"));
        return new ElasticsearchRecordWriter(context, options.getString(ElasticsearchOutputOptionConf.ID_COL_NAME, ""),
                options.getString(ElasticsearchOptionConf.INDEX, ""), options.getString(ElasticsearchOptionConf.DOCUMENT_TYPE, ""), manager);
    }

    @Override
    protected WrapperSetterFactory createWrapperSetterFactory() {
        return new ElasticsearchJsonWrapperSetterManager();
    }
}

class ElasticsearchJsonWrapperSetterManager extends JsonWrapperSetterManager{
    public ElasticsearchJsonWrapperSetterManager() {
        super(DataSourceRole.TARGET);
    }
}

class ElasticsearchRecordWriter extends HerculesRecordWriter<Document> {

    private final List<DocRequest> indexBuffer = new LinkedList<>();
    private final String keyName;
    private final String index;
    private final String docType;
    private final int bufferSizeLimit = 1000;
    private final ElasticsearchManager manager;

    public ElasticsearchRecordWriter(TaskAttemptContext context, String keyName, String index, String docType, ElasticsearchManager manager) {
        super(context);
        this.keyName = keyName;
        this.index = index;
        this.docType = docType;
        this.manager = manager;
    }

    @Override
    protected void innerWrite(HerculesWritable in) throws IOException, InterruptedException {
        // TODO null检查
        BaseWrapper key = in.get(keyName);
        WritableUtils.remove(in.getRow(), keyName);
        Document document;
        try {
            document = wrapperSetterFactory.writeMapWrapper(in.getRow(), new Document(), null);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        String doc = JSON.toJSONString(document);
        indexBuffer.add(new DocRequest(index, docType, key.asString(), doc));
        if (indexBuffer.size() >= bufferSizeLimit) {
            manager.doUpsert(indexBuffer);
            indexBuffer.clear();
        }
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.IGNORE;
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        if (indexBuffer.size()!=0){
            manager.doUpsert(indexBuffer);
            indexBuffer.clear();
        }
    }
}
