package com.xiaohongshu.db.hercules.parquetschema.mr;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
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

    private final TypeBuilderTreeNode result;
    private final RecordWriter<NullWritable, Text> delegate;

    private boolean error = false;
    /**
     * 如果一个map一行都没写，会导致直接把空message type写下去
     */
    private boolean written = false;

    @GeneralAssembly
    private ParquetDataTypeConverter dataTypeConverter;

    public ParquetSchemaRecordWriter(TaskAttemptContext context, RecordWriter<NullWritable, Text> delegate) {
        super(context);
        this.delegate = delegate;
        this.result = new TypeBuilderTreeNode(GENERATED_MESSAGE_NAME, Types.buildMessage(), null, BaseDataType.MAP);
    }

    @Override
    protected void innerWrite(HerculesWritable value) throws IOException, InterruptedException {
        if (!written) {
            written = true;
        }
        try {
            ((ParquetSchemaOutputWrapperManager) wrapperSetterFactory).union(value.getRow(), result);
        } catch (Exception e) {
            error = true;
            LOG.error("Error row: " + value.toString());
            throw new IOException(e);
        }
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.IGNORE;
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        if (written && !error) {
            String messageTypeStr = ParquetSchemaUtils.calculateTree(result, dataTypeConverter).toString();
            LOG.info(String.format("Task [%s] generate message type: %s", context.getTaskAttemptID(), messageTypeStr));
            delegate.write(NullWritable.get(), new Text(messageTypeStr));
            delegate.close(context);
        }
    }
}
