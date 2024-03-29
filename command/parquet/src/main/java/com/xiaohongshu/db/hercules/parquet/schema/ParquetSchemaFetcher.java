package com.xiaohongshu.db.hercules.parquet.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.HiveMetaOptionsConf.HIVE_META_CONNECTION;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.DIR;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetSchemaFetcher extends BaseSchemaFetcher {

    private static final Log LOG = LogFactory.getLog(ParquetSchemaFetcher.class);

    /**
     * 仅仅用于hdfs拿文件
     */
    private final Configuration tmpConfiguration;

    private MessageType messageType = null;

    @Assembly
    private ParquetDataTypeConverter dataTypeConverter;

    /**
     * 抄的sqoop
     *
     * @param file
     * @return
     */
    private boolean isParquet(Path file) {
        // Test target's header to see if it contains magic numbers indicating its
        // file type
        byte[] header = new byte[3];
        FSDataInputStream is = null;
        try {
            FileSystem fs = file.getFileSystem(tmpConfiguration);
            is = fs.open(file);
            is.readFully(header);
        } catch (IOException ioe) {
            // Error reading header or EOF; assume unknown
            LOG.warn(String.format("IOException checking input file [%s] header: " + ioe.getMessage(), file.toString()));
            return false;
        } finally {
            try {
                if (null != is) {
                    is.close();
                }
            } catch (IOException ioe) {
                // ignore; closing.
                LOG.warn("IOException closing input stream: " + ioe.getMessage() + "; ignoring.");
            }
        }

        return header[0] == 'P' && header[1] == 'A' && header[2] == 'R';
    }

    private Path firstParquetOfDir(Path dir, boolean recursive) {
        FileStatus[] fileStatuses;
        try {
            FileSystem fs = FileSystem.get(dir.toUri(), tmpConfiguration);
            if (!fs.exists(dir)) {
                LOG.warn(String.format("Dir [%s] doesn't exist, cannot fetch schema.", dir.toString()));
                return null;
            } else {
                fileStatuses = fs.listStatus(dir);
            }
        } catch (IOException e) {
            throw new SchemaException(e);
        }
        for (FileStatus fileStatus : fileStatuses) {
            Path tmpPath = fileStatus.getPath();
            if (fileStatus.isDirectory()) {
                if (recursive && (tmpPath = firstParquetOfDir(tmpPath, true)) != null) {
                    return tmpPath;
                }
            } else {
                if (isParquet(tmpPath)) {
                    return tmpPath;
                }
            }
        }
        LOG.warn(String.format("There should be at least one parquet file in dir [%s], now zero.", dir.toString()));
        return null;
    }

    private MessageType fetchMessageType() {
        String dir = getOptions().getString(DIR, null);
        try {
            Path first = firstParquetOfDir(new Path(dir), true);
            MessageType res = first == null ? null : ParquetFileReader
                    .readFooter(tmpConfiguration, first)
                    .getFileMetaData()
                    .getSchema();
            if (res == null) {
                throw new RuntimeException("Unable to fetch the column type from parquet file.");
            } else {
                return res;
            }
        } catch (IOException e) {
            throw new SchemaException(e);
        }
    }

    public ParquetSchemaFetcher(GenericOptions options) {
        this(options, new Configuration());
    }

    public ParquetSchemaFetcher(GenericOptions options, Configuration tmpConfiguration) {
        super(options);
        this.tmpConfiguration = tmpConfiguration;

        // 获得parquet schema
        if (getOptions().hasProperty(MESSAGE_TYPE)) {
            messageType = MessageTypeParser.parseMessageType(getOptions().getString(MESSAGE_TYPE, null));
        } else {
            if (getOptions().hasProperty(HIVE_META_CONNECTION)) {
                Map<String, String> typeMap = SchemaUtils.fetchHiveTableSchema(getOptions());
                messageType = ParquetSchemaUtils.generateMessageTypeFromHiveMeta(typeMap);
                LOG.info("The schema converted from hive schema is: " + messageType.toString());
            } else if (options.getOptionsType().isSource()) {
                // 如果作为目标，自动从原文件取schema可能不太合适，毕竟万一上游动了schema，下游感知不到，故仅在作为上游时取
                messageType = fetchMessageType();
                LOG.info("The schema fetched from parquet file is: " + messageType.toString());
            }

            // 顺便偷摸把取出来的parquet message type塞到options里，因为之后还有用，不用屡次取了
            if (messageType != null) {
                getOptions().set(MESSAGE_TYPE, messageType.toString());
            }
        }
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        if (messageType != null) {
            return messageType.getFields()
                    .stream()
                    .map(Type::getName)
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap() {
        if (messageType != null) {
            return dataTypeConverter.convertRowType(messageType);
        } else {
            return null;
        }
    }
}
