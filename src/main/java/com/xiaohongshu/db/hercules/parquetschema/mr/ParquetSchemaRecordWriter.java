package com.xiaohongshu.db.hercules.parquetschema.mr;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils;
import com.xiaohongshu.db.hercules.parquet.schema.TypeBuilderTreeNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.schema.Types;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils.GENERATED_MESSAGE_NAME;

public class ParquetSchemaRecordWriter extends HerculesRecordWriter<TypeBuilderTreeNode> {

    private static final Log LOG = LogFactory.getLog(ParquetSchemaRecordWriter.class);

    private final ParquetSchemaOutputWrapperManager wrapperManager;
    private final TypeBuilderTreeNode result;
    private final RecordWriter<NullWritable, Text> delegate;

    private boolean error = false;
    /**
     * 如果一个map一行都没写，会导致直接把空message type写下去
     */
    private boolean written = false;

    public ParquetSchemaRecordWriter(TaskAttemptContext context, ParquetSchemaOutputWrapperManager wrapperSetterFactory,
                                     RecordWriter<NullWritable, Text> delegate) {
        super(context, wrapperSetterFactory);
        this.wrapperManager = wrapperSetterFactory;
        this.delegate = delegate;
        this.result = new TypeBuilderTreeNode(GENERATED_MESSAGE_NAME, Types.buildMessage(), null, DataType.MAP);
        wrapperManager.setColumnTypeMap(columnTypeMap);
    }

    @Override
    protected void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException {
        value = new HerculesWritable(WritableUtils.copyColumn(value.getRow(), columnNameList, WritableUtils.FilterUnexistOption.IGNORE));
        innerMapWrite(value);
    }

    @Override
    protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException {
        if (!written) {
            written = true;
        }
        try {
            wrapperManager.union(value.getRow(), result);
        } catch (Exception e) {
            error = true;
            LOG.error("Error row: " + value.toString());
            throw new IOException(e);
        }
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        if (written && !error) {
            String messageTypeStr = ParquetSchemaUtils.calculateTree(result, wrapperManager.getConverter()).toString();
            LOG.info(String.format("Task [%s] generate message type: %s", context.getTaskAttemptID(), messageTypeStr));
            delegate.write(NullWritable.get(), new Text(messageTypeStr));
            delegate.close(context);
        }
    }
}
