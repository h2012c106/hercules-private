package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.util.List;
import java.util.Stack;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetRecordReader extends HerculesRecordReader<GroupWithSchemaInfo, ParquetDataTypeConverter> {

    private static final Log LOG = LogFactory.getLog(ParquetRecordReader.class);

    private final ParquetInputWrapperManager wrapperManager;

    private final RecordReader<Void, Group> delegate;

    private MessageType messageType;

    public ParquetRecordReader(ParquetDataTypeConverter converter, RecordReader<Void, Group> delegate,
                               ParquetInputWrapperManager wrapperManager) {
        // 此时还没搞出columnTypeMap
        super(converter, wrapperManager);
        this.delegate = delegate;
        this.wrapperManager = wrapperManager;
    }

    /**
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

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        // 补赋columnTypeMap
        wrapperManager.setColumnTypeMap(columnTypeMap);

        // 不用担心NPE，横竖这里都有了
        messageType = MessageTypeParser.parseMessageType(options.getSourceOptions().getString(MESSAGE_TYPE, null));
        // 处理一下column的筛选
        if (!emptyColumnNameList) {
            MessageType tmpMessageType = Types.buildMessage().named(messageType.getName());
            for (String columnName : columnNameList) {
                MessageType projectedMessageType = projectOneColumn(columnName);
                if (projectedMessageType == null) {
                    LOG.warn(String.format("Can't find the column [%s] in schema.", columnName));
                } else {
                    tmpMessageType = tmpMessageType.union(projectedMessageType);
                }
            }
            messageType = tmpMessageType;
            LOG.info(String.format("The projection message type of column %s is: %s", columnNameList, messageType));
        }
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, messageType.toString());

        delegate.initialize(split, context);
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        return delegate.nextKeyValue();
    }

    @Override
    public HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException {
        try {
            return new HerculesWritable(wrapperManager.groupToMapWrapper(delegate.getCurrentValue(), null));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return delegate.getProgress();
    }

    @Override
    public void innerClose() throws IOException {
        delegate.close();
    }
}
