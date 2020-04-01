package com.xiaohongshu.db.hercules.core.mr;

import com.cloudera.sqoop.config.ConfigurationHelper;
import com.xiaohongshu.db.hercules.common.options.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.exceptions.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.mapper.HerculesMapper;
import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.command.CommandExecutor;
import com.xiaohongshu.db.hercules.core.utils.command.CommandResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.sqoop.util.PerfCounters;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MRJob {

    private static final Log LOG = LogFactory.getLog(MRJob.class);

    private static final String[] MAPREDUCE_MAP_MAX_ATTEMPTS
            = new String[]{"mapreduce.map.maxattempts", "mapred.map.max.attempts"};
    private static final String[] MAPREDUCE_MAP_SPECULATIVE_EXECUTION
            = new String[]{"mapreduce.map.speculative", "mapred.map.tasks.speculative.execution"};

    private BaseAssemblySupplier sourceAssemblySupplier;
    private BaseAssemblySupplier targetAssemblySupplier;
    private WrappingOptions options;

    public MRJob(BaseAssemblySupplier sourceAssemblySupplier,
                 BaseAssemblySupplier targetAssemblySupplier,
                 WrappingOptions options) {
        this.sourceAssemblySupplier = sourceAssemblySupplier;
        this.targetAssemblySupplier = targetAssemblySupplier;
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
            LOG.info(result.getData());
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
            LOG.info(result.getData());
            return;
        }

        command = String.format("yarn logs -applicationId %s -log_files syslog -containerId %s",
                applicationId, containerId);
        result = execCommandWithRetry(command, commandTimeout, maxRetryTime, retryInterval);
        if (result.getCode() != 0) {
            LOG.warn(String.format("Command [%s] fail, %s", command, result.toString()));
            return;
        }
        LOG.info(String.format("The the failed task[%s]'s container's log:", failedAttemptId));
        LOG.info(result.getData());
    }

    public void run(String... args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        new GenericOptionsParser(configuration, args);

        options.toConfiguration(configuration);



        Job job = new Job(configuration);
        job.setJarByClass(MRJob.class);

        sourceAssemblySupplier.getJobContextAsSource().configureInput();
        job.setInputFormatClass(sourceAssemblySupplier.getInputFormatClass());

        targetAssemblySupplier.getJobContextAsSource().configureOutput();
        job.setOutputFormatClass(targetAssemblySupplier.getOutputFormatClass());

        job.setMapperClass(HerculesMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(HerculesWritable.class);

        ConfigurationHelper.setJobNumMaps(job,
                options.getCommonOptions().getInteger(CommonOptionsConf.NUM_MAPPER,
                        CommonOptionsConf.DEFAULT_NUM_MAPPER)
        );
        job.setNumReduceTasks(0);

        configureMRJob(job.getConfiguration());

        sourceAssemblySupplier.getJobContextAsSource().preRun(options.getSourceOptions());
        targetAssemblySupplier.getJobContextAsTarget().preRun(options.getTargetOptions());

        PerfCounters perfCounters = new PerfCounters();
        perfCounters.startClock();
        boolean success = job.waitForCompletion(true);
        perfCounters.stopClock();
        Counters jobCounters = job.getCounters();
        // If the job has been retired, these may be unavailable.
        long numRecords;
        if (null == jobCounters) {
            numRecords = 0;
        } else {
            perfCounters.addBytes(jobCounters.getGroup(HerculesMapper.HERCULES_GROUP_NAME)
                    .findCounter(HerculesMapper.ESTIMATED_BYTE_SIZE_COUNTER_NAME).getValue());
            LOG.info("Transferred " + perfCounters.toString());
            numRecords = ConfigurationHelper.getNumMapOutputRecords(job);
            LOG.info("Retrieved " + numRecords + " records.");
        }

        if (!success) {
            printFailedTaskLog(job);
            throw new MapReduceException("The map reduce job failed.");
        }

        sourceAssemblySupplier.getJobContextAsSource().postRun(options.getSourceOptions());
        targetAssemblySupplier.getJobContextAsTarget().postRun(options.getTargetOptions());
    }
}
