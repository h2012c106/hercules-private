package com.xiaohongshu.db.hercules.elasticsearchv6.mr;

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
import com.xiaohongshu.db.hercules.elasticsearchv6.option.ElasticsearchOptionConf;
import com.xiaohongshu.db.hercules.elasticsearchv6.option.ElasticsearchOutputOptionConf;
import com.xiaohongshu.db.hercules.elasticsearchv6.schema.manager.DocRequest;
import com.xiaohongshu.db.hercules.elasticsearchv6.schema.manager.ElasticsearchManager;
import com.xiaohongshu.db.hercules.serder.json.ser.JsonWrapperSetterManager;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf.ALLOW_SKIP;

public class ElasticsearchOutputFormat extends HerculesOutputFormat<Document> {

    @Options(type = OptionsType.TARGET)
    private GenericOptions options;

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    @Override
    protected HerculesRecordWriter<Document> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        ElasticsearchManager manager = new ElasticsearchManager(options.getString(ElasticsearchOptionConf.ENDPOINT, ""),
                options.getInteger(ElasticsearchOptionConf.PORT, 0), options.getString(ElasticsearchOptionConf.DOCUMENT_TYPE, "doc"));
        return new ElasticsearchRecordWriter(context, options.getString(ElasticsearchOutputOptionConf.ID_COL_NAME, ""),
                options.getString(ElasticsearchOptionConf.INDEX, ""), manager,
                commonOptions.getBoolean(ALLOW_SKIP, false),
                options.getBoolean(ElasticsearchOptionConf.KEEP_ID, false));
    }

    @Override
    protected WrapperSetterFactory createWrapperSetterFactory() {
        return new ElasticsearchJsonWrapperSetterManager();
    }
}

class ElasticsearchRecordWriter extends HerculesRecordWriter<Document> {


    private final List<DocRequest> indexBuffer = new LinkedList<>();
    private final String keyName;
    private final String index;
    private final ElasticsearchManager manager;
    private boolean allowSkip;
    private final boolean keepId;
    private int bufByteSize = 0;
    private int bufByteSizeLimit = 400 * 1024;

    public ElasticsearchRecordWriter(TaskAttemptContext context, String keyName, String index, ElasticsearchManager manager, boolean allowSkip, boolean keepId) {
        super(context);
        this.keyName = keyName;
        this.index = index;
        this.manager = manager;
        this.allowSkip = allowSkip;
        this.keepId = keepId;
    }

    @Override
    protected void innerWrite(HerculesWritable in) throws IOException, InterruptedException {

        BaseWrapper key = in.get(keyName);
        if (key == null) {
            // 默认不允许key出现空值
            if (!allowSkip) {
                throw new RuntimeException(String.format("Meaningless row: %s", in.toString()));
            }
            // 显示指定可以skip后，则直接略过该行数据
            return;
        }
        if (!keepId) {
            WritableUtils.remove(in.getRow(), keyName);
        }
        Document document;
        try {
            document = wrapperSetterFactory.writeMapWrapper(in.getRow(), new Document(), null);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        String doc = JSON.toJSONString(document);
        indexBuffer.add(new DocRequest(index, key.asString(), doc));
        bufByteSize += doc.getBytes().length;
        if (bufByteSize >= bufByteSizeLimit) {
            manager.doUpsert(indexBuffer);
            bufByteSize = 0;
            indexBuffer.clear();
        }
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.IGNORE;
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        if (indexBuffer.size() != 0) {
            manager.doUpsert(indexBuffer);
            indexBuffer.clear();
        }
    }
}
