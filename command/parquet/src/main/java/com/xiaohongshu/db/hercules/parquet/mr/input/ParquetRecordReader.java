package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetRecordReader extends HerculesRecordReader<GroupWithSchemaInfo> {

    private static final Log LOG = LogFactory.getLog(ParquetRecordReader.class);

    private RecordReader<Void, Group> delegate = null;
    /**
     * 负责生成delegate reader
     */
    private final ExampleInputFormat delegateInputFormat;

    private MessageType messageType;

    private int combinedSplitSeq = 0;
    private List<FileSplit> combinedSplitList = new ArrayList<>(0);
    private TaskAttemptContext context = null;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    @SchemaInfo
    private Schema schema;

    public ParquetRecordReader(TaskAttemptContext context, ExampleInputFormat delegateInputFormat) {
        // 此时还没搞出columnTypeMap
        super(context);
        this.delegateInputFormat = delegateInputFormat;
    }

    /**
     * 依次把单列映射成MessageType，再在外面拼装。
     * 由于GroupType本身不支持就地地加Type，所以用栈做类似的效果
     * 先把沿途Group压栈，随后依次出栈，并构建一个拷贝GroupType，值仅有一个为上一个出栈的元素
     *
     * @param columnName
     * @return
     */
    private MessageType projectOneColumn(String columnName) {
        WritableUtils.ColumnSplitResult columnNameList = WritableUtils.splitColumnWrapped(columnName);

        String finalColumn = columnNameList.getFinalColumn();
        List<String> groupColumnList = columnNameList.getParentColumnList();

        Stack<GroupType> columnPath = new Stack<>();
        GroupType tmpFoundGroupType = messageType;

        // 把到底层column为止的沿途groupType统统压栈
        for (String tmpColumnName : groupColumnList) {
            if (!tmpFoundGroupType.containsField(tmpColumnName)) {
                return null;
            }
            Type tmpType = tmpFoundGroupType.getType(tmpColumnName);
            if (tmpType.isPrimitive()) {
                return null;
            }
            tmpFoundGroupType = (GroupType) tmpType;
            columnPath.push(tmpFoundGroupType);
        }

        if (!tmpFoundGroupType.containsField(finalColumn)) {
            return null;
        }
        Type tmpValue = tmpFoundGroupType.getType(finalColumn);
        // 在回溯时，所有tmpValue的field名一定存在于上层GroupType，这点在上一个循环中做了保证
        while (!columnPath.empty()) {
            GroupType topGroupType = columnPath.pop();
            tmpValue = Types.buildGroup(topGroupType.getRepetition())
                    .addField(tmpValue)
                    .named(topGroupType.getName());
        }
        return Types.buildMessage().addField(tmpValue).named(messageType.getName());
    }

    private void initializeDelegate() throws IOException, InterruptedException {
        LOG.info("Combined split seq: " + combinedSplitSeq);
        LOG.info("Combined split: " + combinedSplitList.get(combinedSplitSeq));
        if (delegate != null) {
            delegate.close();
        }
        delegate = delegateInputFormat.createRecordReader(combinedSplitList.get(combinedSplitSeq), context);
        delegate.initialize(combinedSplitList.get(combinedSplitSeq), context);
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 不用担心NPE，横竖这里都有了
        messageType = MessageTypeParser.parseMessageType(options.getString(MESSAGE_TYPE, null));
        // 处理一下column的筛选
        if (!schema.getColumnNameList().isEmpty()) {
            MessageType tmpMessageType = Types.buildMessage().named(messageType.getName());
            for (String columnName : schema.getColumnNameList()) {
                MessageType projectedMessageType = projectOneColumn(columnName);
                if (projectedMessageType == null) {
                    LOG.warn(String.format("Can't find the column [%s] in schema.", columnName));
                } else {
                    tmpMessageType = tmpMessageType.union(projectedMessageType);
                }
            }
            messageType = tmpMessageType;
            LOG.info(String.format("The projection message type of column %s is: %s", schema.getColumnNameList(), messageType));
        }
        context.getConfiguration().set(ReadSupport.PARQUET_READ_SCHEMA, messageType.toString());

        this.context = context;
        this.combinedSplitList = ((ParquetCombinedInputSplit) split).getCombinedInputSplitList();
        initializeDelegate();
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        if (delegate.nextKeyValue()) {
            return true;
        } else {
            if (++combinedSplitSeq < combinedSplitList.size()) {
                initializeDelegate();
                // 防止下一个split无数据
                return innerNextKeyValue();
            } else {
                return false;
            }
        }
    }

    @Override
    public HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException {
        try {
            return new HerculesWritable(((ParquetInputWrapperManager) wrapperGetterFactory).groupToMapWrapper(delegate.getCurrentValue(), null));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        BigDecimal splitSeq = BigDecimal.valueOf(combinedSplitSeq);
        BigDecimal splitSize = BigDecimal.valueOf(combinedSplitList.size());
        BigDecimal tmpDelegateProgress = BigDecimal.valueOf(delegate.getProgress());
        BigDecimal base = splitSeq.divide(splitSize, 5, BigDecimal.ROUND_DOWN);
        BigDecimal add = tmpDelegateProgress.divide(splitSize, 5, BigDecimal.ROUND_DOWN);
        return base.add(add).floatValue();
    }

    @Override
    public void innerClose() throws IOException {
        delegate.close();
    }
}
