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
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;

public final class StatUtils {

    private static final Log LOG = LogFactory.getLog(StatUtils.class);

    private static final String STAT_DIR = "/tmp/hercules-stat/";

    private static final Duration TIME_OUT = Duration.ofSeconds(60);

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

    private static void innerRecord(TaskAttemptContext context, String statName, String value) {
        String dstFileName = STAT_DIR + context.getJobID() + "/" + context.getTaskAttemptID() + "/" + statName;
        LOG.debug(String.format("Logging stat <%s> to: %s", value, dstFileName));
        Path dstFilePath = new Path(dstFileName);
        FileSystem fs = null;
        FSDataOutputStream outputStream = null;
        try {
            fs = dstFilePath.getFileSystem(context.getConfiguration());
            outputStream = fs.create(dstFilePath);
            outputStream.writeBytes(value);
        } catch (IOException e) {
            LOG.warn(String.format("Logging stat <%s> to %s failed: %s", value, dstFileName, e.getMessage()));
        } finally {
            close(outputStream);
        }
    }

    public static void record(TaskAttemptContext context, String statName, String value) {
        FutureTask<Void> recordTask = new FutureTask<>(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                innerRecord(context, statName, value);
                return null;
            }
        });
        new Thread(recordTask).start();
        try {
            recordTask.get(TIME_OUT.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn(String.format("Logging stat <%s> failed: %s", value, e.getMessage()));
        } catch (TimeoutException e) {
            LOG.warn(String.format("Logging stat <%s> timeout: %s", value, TIME_OUT.toString()));
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

    private static Map<String, Map<String, String>> innerGetAndClear(Job job) {
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
        } catch (IOException e) {
            LOG.warn(String.format("Reading stat map from %s failed: %s", srcDirName, e.getMessage()));
            return Collections.emptyMap();
        } finally {
            if (fs != null) {
                try {
                    fs.delete(srcDirPath, true);
                } catch (IOException e) {
                    LOG.warn("Delete stat files failed: " + e.getMessage());
                }
            }
        }
    }

    public static Map<String, Map<String, String>> getAndClear(Job job) {
        FutureTask<Map<String, Map<String, String>>> recordTask = new FutureTask<>(new Callable<Map<String, Map<String, String>>>() {
            @Override
            public Map<String, Map<String, String>> call() throws Exception {
                return innerGetAndClear(job);
            }
        });
        new Thread(recordTask).start();
        try {
            return recordTask.get(TIME_OUT.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn(String.format("Reading stat map failed: %s", e.getMessage()));
            return Collections.emptyMap();
        } catch (TimeoutException e) {
            LOG.warn(String.format("Reading stat map timeout: %s", TIME_OUT.toString()));
            return Collections.emptyMap();
        }
    }

}
