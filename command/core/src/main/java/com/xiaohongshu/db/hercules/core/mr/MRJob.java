package com.xiaohongshu.db.hercules.core.mr;

import com.cloudera.sqoop.config.ConfigurationHelper;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.mapper.HerculesMapper;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.StatUtils;
import com.xiaohongshu.db.hercules.core.utils.command.CommandExecutor;
import com.xiaohongshu.db.hercules.core.utils.command.CommandResult;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesCounter;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf.JOB_NAME;

public class MRJob {

    private static final Log LOG = LogFactory.getLog(MRJob.class);

    private static final String[] MAPREDUCE_MAP_MAX_ATTEMPTS
            = new String[]{"mapreduce.map.maxattempts", "mapred.map.max.attempts"};
    private static final String[] MAPREDUCE_MAP_SPECULATIVE_EXECUTION
            = new String[]{"mapreduce.map.speculative", "mapred.map.tasks.speculative.execution"};
    private static final String[] MAPREDUCE_USER_CLASSPATH_FIRST
            = new String[]{"mapreduce.user.classpath.first", "mapreduce.task.classpath.user.precedence"};

    private static final String TMP_JARS_PROP = "tmpjars";

    private WrappingOptions options;

    private List<String> jarList;

    @Assembly(role = DataSourceRole.SOURCE)
    private Class<? extends HerculesInputFormat<?>> inputFormatClass;

    @Assembly(role = DataSourceRole.TARGET)
    private Class<? extends HerculesOutputFormat<?>> outputFormatClass;

    @Assembly(role = DataSourceRole.SOURCE)
    private MRJobContext jobContextAsSource;

    @Assembly(role = DataSourceRole.TARGET)
    private MRJobContext jobContextAsTarget;

    public MRJob(WrappingOptions options) {
        this.options = options;
    }

    private void configure(Configuration configuration, String value, String... keys) {
        for (String key : keys) {
            configuration.set(key, value);
        }
    }

    /**
     * 配置一些必须的参数，比如max attempts
     *
     * @param configuration
     */
    private void configureMRJob(Configuration configuration) {
        configure(configuration, Integer.toString(1), MAPREDUCE_MAP_MAX_ATTEMPTS);
        configure(configuration, Boolean.toString(false), MAPREDUCE_MAP_SPECULATIVE_EXECUTION);
        // configure(configuration, Boolean.toString(true), MAPREDUCE_USER_CLASSPATH_FIRST);
    }


    private CommandResult execCommandWithRetry(String command, long commandTimeout,
                                               int maxRetryTime, long retryInterval) throws InterruptedException {
        int retryTime = 0;
        CommandResult result = null;
        while (retryTime <= maxRetryTime && (result == null || result.getCode() != 0)) {
            result = CommandExecutor.execute(command, commandTimeout);
            if (result.getCode() == 0) {
                break;
            }
            ++retryTime;
            LOG.warn(String.format("Command [%s] return %s, retry after %dms: %d/%d", command, result.toString(),
                    retryInterval, retryTime, maxRetryTime));
            Thread.sleep(retryInterval);
        }
        return result;
    }

    private void printFailedTaskLog(Job job) throws IOException, InterruptedException {
        long commandTimeout = 5 * 60 * 1000;
        int maxRetryTime = 3;
        long retryInterval = 2 * 1000;
        // 用yarn logs ... 命令查详细日志
        String applicationId = job.getJobID().toString().replace("job", "application");
        String failedAttemptId = null;
        String containerId = null;

        String regex;
        Pattern pattern;
        Matcher matcher;

        String command;
        CommandResult result;

        // 把获得全部container错误日志
        command = String.format("yarn logs -applicationId %s -log_files syslog", applicationId);
        result = execCommandWithRetry(command, commandTimeout, maxRetryTime, retryInterval);
        if (result.getCode() != 0) {
            LOG.warn(String.format("Command [%s] fail, %s", command, result.toString()));
            return;
        }

        for (TaskCompletionEvent event : job.getTaskCompletionEvents(0)) {
            if (event.getTaskStatus().equals(TaskCompletionEvent.Status.TIPFAILED) ||
                    event.getTaskStatus().equals(TaskCompletionEvent.Status.FAILED)) {
                failedAttemptId = event.getTaskAttemptId().toString();
            }
        }
        if (failedAttemptId == null) {
            regex = "Diagnostics report from (attempt_.+): Error";
            pattern = Pattern.compile(regex);
            matcher = pattern.matcher(result.getData());
            if (matcher.find()) {
                failedAttemptId = matcher.group(1);
            }
        }
        if (failedAttemptId == null) {
            LOG.warn("Cannot find the failed task, simply show the full log:");
            LOG.error(result.getData());
            return;
        }

        regex = String.format("Assigned container (container_.+) to %s", failedAttemptId);
        pattern = Pattern.compile(regex);
        matcher = pattern.matcher(result.getData());
        if (matcher.find()) {
            containerId = matcher.group(1);
            LOG.info(String.format("Find the container of task[%s]: %s", failedAttemptId, containerId));
        }
        if (containerId == null) {
            LOG.warn(String.format("Cannot find the failed task[%s]'s container, simply show the full log:",
                    failedAttemptId));
            LOG.error(result.getData());
            return;
        }

        command = String.format("yarn logs -applicationId %s -log_files syslog -containerId %s",
                applicationId, containerId);
        result = execCommandWithRetry(command, commandTimeout, maxRetryTime, retryInterval);
        if (result.getCode() != 0) {
            LOG.warn(String.format("Command [%s] fail, %s", command, result.toString()));
            return;
        }
        LOG.error(String.format("The the failed task[%s]'s container's log:", failedAttemptId));
        LOG.error(result.getData());
    }

    private String formatByte(double byteSize) {
        NumberFormat fmt = NumberFormat.getInstance();
        fmt.setMaximumFractionDigits(4);
        double val;
        String scale;
        if (byteSize > 1.073741824E9D) {
            val = byteSize / 1.073741824E9D;
            scale = "GB";
        } else if (byteSize > 1048576.0D) {
            val = byteSize / 1048576.0D;
            scale = "MB";
        } else if (byteSize > 1024.0D) {
            val = byteSize / 1024.0D;
            scale = "KB";
        } else {
            val = byteSize;
            scale = "B";
        }

        return fmt.format(val) + scale;
    }

    private long getValue(Job job, HerculesCounter counter) throws IOException {
        return job.getCounters().getGroup(HerculesStatus.GROUP_NAME).findCounter(counter.getCounterName()).getValue();
    }

    private String qualifyTmpJar(String tmpPath, Configuration conf) {
        FileSystem fs = null;
        try {
            fs = FileSystem.getLocal(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Path(tmpPath).makeQualified(fs).toString();
    }

    public void setJarList(List<String> jarList) {
        this.jarList = jarList;
    }

    private double calcEuclideanDistance(List<Double> x, List<Double> y) {
        if (x.size() != y.size()) {
            throw new RuntimeException(String.format("Unequal list for Euclidean distance calc: %d vs %d", x.size(), y.size()));
        }
        double powSum = 0.0d;
        for (int i = 0; i < x.size(); ++i) {
            powSum += Math.pow(x.get(i) - y.get(i), 2);
        }
        return Math.sqrt(powSum);
    }

    private List<Double> generateWorstSkewCase(double total, int listSize) {
        List<Double> res = new ArrayList<>(listSize);
        res.add(total);
        for (int i = 1; i < listSize; ++i) {
            res.add(0.0d);
        }
        return res;
    }

    private List<Double> generateBestSkewCase(double total, int listSize) {
        double average = total / listSize;
        List<Double> res = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; ++i) {
            res.add(average);
        }
        return res;
    }

    private double calcSkew(List<Double> mapNumList) {
        if (mapNumList.size() <= 1) {
            return 0.0d;
        }
        double totalNum = mapNumList.stream().reduce(0.0d, Double::sum);

        List<Double> worstSkewCase = generateWorstSkewCase(totalNum, mapNumList.size());
        List<Double> bestSkewCase = generateBestSkewCase(totalNum, mapNumList.size());

        double worstSkew = calcEuclideanDistance(worstSkewCase, bestSkewCase);
        // 判断分母为0
        if (Math.abs(worstSkew - 0.0d) < 1e-6) {
            return 0.0d;
        } else {
            double currentSkew = calcEuclideanDistance(mapNumList, bestSkewCase);
            return currentSkew / worstSkew;
        }
    }

    public void run(String... args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        configureMRJob(configuration);

        new GenericOptionsParser(configuration, args);

        options.toConfiguration(configuration);

        Job job = new Job(configuration);
        if (options.getCommonOptions().hasProperty(JOB_NAME)) {
            job.setJobName(options.getCommonOptions().getString(JOB_NAME, null));
        }
        job.setJarByClass(MRJob.class);

        jobContextAsSource.configureJob(job);
        job.setInputFormatClass(inputFormatClass);

        jobContextAsTarget.configureJob(job);
        job.setOutputFormatClass(outputFormatClass);

        job.setMapperClass(HerculesMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(HerculesWritable.class);

        ConfigurationHelper.setJobNumMaps(job,
                options.getCommonOptions().getInteger(CommonOptionsConf.NUM_MAPPER,
                        CommonOptionsConf.DEFAULT_NUM_MAPPER)
        );
        job.setNumReduceTasks(0);

        // 设置libjar
        List<String> tmpJarList = new LinkedList<>();
        String tmpJars = job.getConfiguration().get(TMP_JARS_PROP);
        if (!StringUtils.isEmpty(tmpJars)) {
            tmpJarList.add(tmpJars);
        }
        tmpJarList.addAll(jarList.stream()
                .map(jarName -> qualifyTmpJar(jarName, job.getConfiguration()))
                .collect(Collectors.toList()));
//        LOG.info("System path.separator: " + System.getProperty("path.separator"));
//        String tmpJarsStr = StringUtils.join(tmpJarList, System.getProperty("path.separator"));
        String tmpJarsStr = StringUtils.join(tmpJarList, ",");
        job.getConfiguration().set(TMP_JARS_PROP, tmpJarsStr);
        LOG.debug(String.format("Property [%s]: %s", TMP_JARS_PROP, tmpJarsStr));

        jobContextAsSource.preRun();
        jobContextAsTarget.preRun();

        boolean success = job.waitForCompletion(true);
        double runMilliSec = (job.getFinishTime() - job.getStartTime()) / 1000.0;

        try {
            Map<String, Map<String, String>> statMap = StatUtils.getAndClear(job);
            LOG.info("Stats collected from map: " + statMap);
            final NumberFormat numberFormat = NumberFormat.getPercentInstance();
            numberFormat.setMinimumFractionDigits(2);
            numberFormat.setRoundingMode(RoundingMode.HALF_UP);
            List<Double> mapNumList = statMap.values()
                    .stream()
                    .map(map -> Double.parseDouble(map.getOrDefault(HerculesCounter.READ_RECORDS.getCounterName(), "0")))
                    .collect(Collectors.toList());
            LOG.info(String.format("The skew rate for %d map(s) is: %s.", mapNumList.size(), numberFormat.format(calcSkew(mapNumList))));
        } catch (Exception e) {
            LOG.info(String.format("The skew rate fetch failed: %s", e.getMessage()));
        }

        long readNum = getValue(job, HerculesCounter.READ_RECORDS);
        long writeNum = getValue(job, HerculesCounter.WRITE_RECORDS);
        long readBytes = getValue(job, HerculesCounter.ESTIMATED_MAPPER_READ_BYTE_SIZE);
        long writeBytes = getValue(job, HerculesCounter.ESTIMATED_MAPPER_WRITE_BYTE_SIZE);
        LOG.info(String.format("Use %.4fs in total.", runMilliSec));
        LOG.info(String.format("Read %d records (%.4f row/s) estimated at %s (%s/s).",
                readNum, (double) readNum / runMilliSec,
                formatByte(readBytes), formatByte((double) readBytes / runMilliSec)));
        LOG.info(String.format("Written %d records (%.4f row/s) estimated at %s (%s/s).",
                writeNum, (double) writeNum / runMilliSec,
                formatByte(writeBytes), formatByte((double) writeBytes / runMilliSec)));

        if (!success) {
            TimeUnit.SECONDS.sleep(2);
            printFailedTaskLog(job);
            throw new MapReduceException("The map reduce job failed.");
        }

        jobContextAsSource.postRun();
        jobContextAsTarget.postRun();
    }
}
