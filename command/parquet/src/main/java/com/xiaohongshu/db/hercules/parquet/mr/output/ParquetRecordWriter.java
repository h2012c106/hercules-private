package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
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

    private final SimpleGroupFactory groupFactory;

    public ParquetRecordWriter(TaskAttemptContext context, RecordWriter<Void, Group> delegate) {
        super(context);
        this.delegate = delegate;

        // 不用担心NPE，横竖这里都有了
        MessageType messageType = MessageTypeParser.parseMessageType(options.getTargetOptions().getString(MESSAGE_TYPE, null));
        groupFactory = new SimpleGroupFactory(messageType);
    }

    @Override
    protected void innerWrite(HerculesWritable value) throws IOException, InterruptedException {
        try {
            delegate.write(null, wrapperSetterFactory.writeMapWrapper(value.getRow(), groupFactory.newGroup(), null));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.IGNORE;
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.close(context);
    }
}
