package com.xiaohongshu.db.hercules.core.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class StatUtils {

    private static final Log LOG = LogFactory.getLog(StatUtils.class);

    private static final String STAT_DIR = "/tmp/hercules-stat/";

    private static void close(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    LOG.warn(String.format("Close %s failed: %s", closeable, e.getMessage()));
                }
            }
        }
    }

    public static void record(TaskAttemptContext context, String statName, String value) throws IOException {
        String dstFileName = STAT_DIR + context.getJobID() + "/" + context.getTaskAttemptID() + "/" + statName;
        LOG.debug(String.format("Logging stat <%s> to: %s", value, dstFileName));
        Path dstFilePath = new Path(dstFileName);
        FileSystem fs = dstFilePath.getFileSystem(context.getConfiguration());
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(dstFilePath);
            outputStream.writeBytes(value);
        } finally {
            close(outputStream);
        }
    }

    private static String readOneFile(FileSystem fs, Path path) throws IOException {
        StringBuilder result = new StringBuilder();
        FSDataInputStream inputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            inputStream = fs.open(path);
            inputStreamReader = new InputStreamReader(inputStream);
            bufferedReader = new BufferedReader(inputStreamReader);
            String tmpLine;
            while ((tmpLine = bufferedReader.readLine()) != null) {
                result.append(tmpLine).append("\n");
            }
            return result.toString().trim();
        } finally {
            close(bufferedReader, inputStreamReader, inputStream);
        }
    }

    public static Map<String, Map<String, String>> getAndClear(Job job) throws IOException {
        Map<String, Map<String, String>> res = new LinkedHashMap<>();
        String srcDirName = STAT_DIR + job.getJobID() + "/";
        LOG.debug(String.format("Reading stat map from: %s", srcDirName));
        Path srcDirPath = new Path(srcDirName);
        FileSystem fs = null;
        try {
            fs = srcDirPath.getFileSystem(job.getConfiguration());
            if (fs.exists(srcDirPath)) {
                for (FileStatus taskAttempt : fs.listStatus(srcDirPath)) {
                    Path taskAttemptPath = taskAttempt.getPath();
                    String taskAttemptId = taskAttemptPath.getName();
                    for (FileStatus stat : fs.listStatus(taskAttemptPath)) {
                        Path statPath = stat.getPath();
                        String statName = statPath.getName();
                        String value = readOneFile(fs, statPath);
                        res.computeIfAbsent(taskAttemptId, key -> new LinkedHashMap<>()).put(statName, value);
                    }
                }
                return res;
            } else {
                return Collections.emptyMap();
            }
        } finally {
            if (fs != null) {
                fs.delete(srcDirPath, true);
            }
        }
    }

}
