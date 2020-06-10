package com.xiaohongshu.db.hercules.parquetschema.mr;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.schema.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils.GENERATED_MESSAGE_NAME;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.DIR;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;
import static com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf.TYPE_AUTO_UPGRADE;

public class ParquetSchemaOutputMRJobContext implements MRJobContext {

    private static final Log LOG = LogFactory.getLog(ParquetSchemaOutputMRJobContext.class);

    private static final String GENERATED_FILE_NAME = "HERCULES_GENERATED_SCHEMA";

    @Override
    public void configureJob(Job job, WrappingOptions options) {
        Path targetDir = new Path(options.getTargetOptions().getString(DIR, null));
        try {
            FileSystem fs = targetDir.getFileSystem(job.getConfiguration());
            if (fs.exists(targetDir)) {
                fs.delete(targetDir, true);
                LOG.info("Destination directory " + targetDir + " deleted.");
            } else {
                LOG.info("Destination directory " + targetDir + " is not present, "
                        + "hence not deleting.");
            }
        } catch (IOException e) {
            LOG.error("Delete target dir failed.");
            throw new RuntimeException(e);
        }
        TextOutputFormat.setOutputPath(job, targetDir);
        TextOutputFormat.setCompressOutput(job, false);
    }

    @Override
    public void preRun(WrappingOptions options) {

    }

    private ParquetDataTypeConverter getConverter(GenericOptions options) {
        SchemaStyle schemaStyle = SchemaStyle.valueOfIgnoreCase(options.getString(SCHEMA_STYLE, null));
        switch (schemaStyle) {
            case SQOOP:
                return ParquetSqoopDataTypeConverter.getInstance();
            case HIVE:
                return ParquetHiveDataTypeConverter.getInstance();
            case ORIGINAL:
                return ParquetHerculesDataTypeConverter.getInstance();
            default:
                throw new RuntimeException();
        }
    }

    private String readFile(Path file, FileSystem fs) throws IOException {
        FSDataInputStream inputStream = null;
        InputStreamReader streamReader = null;
        BufferedReader bufferedReader = null;
        try {
            inputStream = fs.open(file);
            streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            bufferedReader = new BufferedReader(streamReader);
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
            return String.join("\n", lines);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.warn("BufferedReader failed to close: " + e.getMessage());
                }
            }
            if (streamReader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    LOG.warn("InputStreamReader failed to close: " + e.getMessage());
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.warn("FSDataInputStream failed to close: " + e.getMessage());
                }
            }
        }
    }

    private MessageType getFileParquetSchema(Path file, FileSystem fs) throws IOException {
        if (fs.exists(file) && fs.getFileStatus(file).isFile()) {
            String fileMessage = readFile(file, fs);
            try {
                return MessageTypeParser.parseMessageType(fileMessage);
            } catch (Exception e) {
                LOG.info(String.format("File %s doesn't contain a parquet schema, skip, exception: %s", file.toString(), e.getMessage()));
                return null;
            }
        } else {
            return null;
        }
    }

    private List<PathWithSchema> getParquetSchemaFileList(Path dir, FileSystem fs) throws IOException {
        if (fs.exists(dir) && fs.getFileStatus(dir).isDirectory()) {
            List<PathWithSchema> res = new ArrayList<>();
            for (FileStatus status : fs.listStatus(dir)) {
                Path filePath = status.getPath();
                MessageType messageType = getFileParquetSchema(filePath, fs);
                if (messageType != null) {
                    res.add(new PathWithSchema(filePath, messageType));
                }
            }
            return res;
        } else {
            return Collections.emptyList();
        }
    }

    private static class PathWithSchema {
        private Path path;
        private MessageType messageType;

        public PathWithSchema(Path path, MessageType messageType) {
            this.path = path;
            this.messageType = messageType;
        }

        public Path getPath() {
            return path;
        }

        public MessageType getMessageType() {
            return messageType;
        }
    }

    private int deleteFiles(List<Path> fileList, FileSystem fs) throws IOException {
        int deleteNum = 0;
        for (Path file : fileList) {
            if (fs.exists(file)) {
                fs.delete(file, false);
                LOG.info("Tmp file " + file.toString() + " deleted.");
                ++deleteNum;
            }
        }
        return deleteNum;
    }

    private void writeFile(Path file, String content, FileSystem fs) throws IOException {
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(file);
            outputStream.writeBytes(content);
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    LOG.warn("FSDataOutputStream failed to close: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void postRun(WrappingOptions options) {
        final ParquetDataTypeConverter converter = getConverter(options.getTargetOptions());
        Path targetDir = new Path(options.getTargetOptions().getString(DIR, null));
        Path targetFile = Path.mergePaths(targetDir, new Path("/" + GENERATED_FILE_NAME));
        boolean typeAutoUpgrade = options.getTargetOptions().getBoolean(TYPE_AUTO_UPGRADE, false);

        try {
            FileSystem fs = targetDir.getFileSystem(new Configuration());
            List<PathWithSchema> pathWithSchemaList = getParquetSchemaFileList(targetDir, fs);
            if (pathWithSchemaList.size() == 0) {
                throw new RuntimeException("Cannot fetch the schema file generated by map task.");
            }
            LOG.info(String.format("Fetch %d schema file(s) in %s.", pathWithSchemaList.size(), targetDir.toString()));

            TypeBuilderTreeNode resTree = new TypeBuilderTreeNode(GENERATED_MESSAGE_NAME, Types.buildMessage(), null, BaseDataType.MAP);
            List<TypeBuilderTreeNode> fileTreeList = pathWithSchemaList.stream()
                    .map(pathWithSchema -> {
                        MessageType messageType = pathWithSchema.getMessageType();
                        return ParquetSchemaUtils.buildTree(messageType, null, converter);
                    })
                    .collect(Collectors.toList());
            for (int i = 0; i < fileTreeList.size(); ++i) {
                TypeBuilderTreeNode fileTree = fileTreeList.get(i);
                ParquetSchemaUtils.unionMapTree(resTree, fileTree, typeAutoUpgrade, i == 0, false);
            }
            String resSchemaStr = ParquetSchemaUtils.calculateTree(resTree, converter).toString();
            LOG.info("The final calculated schema is: " + resSchemaStr);

            LOG.info(String.format("Cleaned %d tmp file(s).",
                    deleteFiles(pathWithSchemaList.stream()
                            .map(PathWithSchema::getPath)
                            .collect(Collectors.toList()), fs)));

            writeFile(targetFile, resSchemaStr, fs);
            LOG.info("Schema has written to file: " + targetFile.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
