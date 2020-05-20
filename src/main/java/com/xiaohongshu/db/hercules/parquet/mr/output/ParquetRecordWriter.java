package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetRecordWriter extends HerculesRecordWriter<Group> {

    private final RecordWriter<Void, Group> delegate;

    private final ParquetOutputWrapperManager wrapperManager;
    private final SimpleGroupFactory groupFactory;

    public ParquetRecordWriter(TaskAttemptContext context, RecordWriter<Void, Group> delegate,
                               ParquetOutputWrapperManager wrapperSetterFactory) {
        super(context, wrapperSetterFactory);
        this.delegate = delegate;
        wrapperManager = wrapperSetterFactory;
        wrapperManager.setColumnTypeMap(columnTypeMap);

        // 不用担心NPE，横竖这里都有了
        MessageType messageType = MessageTypeParser.parseMessageType(options.getTargetOptions().getString(MESSAGE_TYPE, null));
        groupFactory = new SimpleGroupFactory(messageType);
    }

    @Override
    protected void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException {
        try {
            delegate.write(null, wrapperManager.mapWrapperToGroup(value.getRow(), groupFactory.newGroup(), null));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException {
        // 到这一步，前面一定出了schema了，也就是column信息百分百有了
        throw new UnsupportedOperationException("Unexpected to deal with column-unspecified parquet.");
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.close(context);
    }
}
