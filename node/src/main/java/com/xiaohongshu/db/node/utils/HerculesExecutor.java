package com.xiaohongshu.db.node.utils;

import com.xiaohongshu.db.node.service.TaskStatusReplyService;
import com.xiaohongshu.db.node.service.YarnService;
import com.xiaohongshu.db.share.entity.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HerculesExecutor extends CommandExecutor {

    private final Task task;
    private final TaskStatusReplyService replyService;
    private final YarnService yarnService;
    private final String jobName;

    private Long startTime = null;

    private final HerculesReader reader = new HerculesReader();
    private volatile String applicationId = null;

    public HerculesExecutor(Task task, String jobName, TaskStatusReplyService replyService, YarnService yarnService) {
        super(task.getCommand());
        this.task = task;
        this.replyService = replyService;
        this.yarnService = yarnService;
        this.jobName = jobName;
    }

    @Override
    protected Logger getLog() {
        return LoggerFactory.getLogger(HerculesExecutor.class);
    }

    public Task getTask() {
        return task;
    }

    public String getApplicationId() {
        return applicationId;
    }

    @Override
    public CommandExecutorResult call() {
        try {
            // 使日志文件打到对应taskId的文件夹下
            MDC.put("taskId", Long.toString(task.getId()));

            replyService.updateStatus(task.getId(), Task.TaskStatus.RUNNING);

            startTime = System.currentTimeMillis();
            replyService.setStartTime(task.getId());

            return new HerculesExecutorResult(super.call(), task);
        } catch (Exception e) {
            getLog().error("Exception when call command: " + task.getCommand(), e);
            throw e;
        } finally {
            MDC.remove("taskId");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // 使日志文件打到对应taskId的文件夹下
            MDC.put("taskId", Long.toString(task.getId()));
            getLog().info(String.format("Killing hercules task [%d]...", task.getId()));
            super.close();
        } finally {
            MDC.remove("taskId");
        }
    }

    @Override
    protected void readLine(String line) {
        reader.readLine(line);
    }

    @Override
    protected void innerAfterDone(int exitCode) {
        replyService.setTotalTimeMs(task.getId(), System.currentTimeMillis() - startTime);
        Task.TaskStatus status = exitCode == 0 ? Task.TaskStatus.SUCCESS : Task.TaskStatus.ERROR;
        replyService.updateStatus(task.getId(), status);
        replyService.end(task.getId(), status);
    }

    @Override
    protected void innerAfterKilled() throws IOException {
        if (startTime != null) {
            replyService.setTotalTimeMs(task.getId(), System.currentTimeMillis() - startTime);
        }
        Task.TaskStatus status = Task.TaskStatus.KILLED;
        replyService.updateStatus(task.getId(), Task.TaskStatus.KILLED);
        replyService.end(task.getId(), status);
        // 杀完之后把application也给杀了
        if (applicationId != null) {
            try {
                yarnService.kill(applicationId);
            } catch (Exception e) {
                throw new IOException("Application killed fail, be sure to close it manually: " + applicationId, e);
            }
        } else {
            try {
                List<String> applicationList = yarnService.findApplicationIdByName(jobName);
                if (applicationList.size() == 1) {
                    applicationId = applicationList.get(0);
                    yarnService.kill(applicationId);
                } else if (applicationList.size() > 1) {
                    throw new RuntimeException(String.format("The job [%s] has more than one mr task running: %d.", jobName, applicationList.size()));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private static final Pattern MAP_NUM_PATTERN = Pattern.compile("^.*number of splits:(\\d+)$");
    private static final Pattern APPLICATION_ID_PATTERN = Pattern.compile("^.*Submitted application (application\\w+)$");
    private static final Pattern MR_LOG_URL_PATTERN = Pattern.compile("^.*The url to track the job: (.+)$");
    private static final Pattern ESTIMATED_BYTE_SIZE_PATTERN = Pattern.compile("^\\s+Estimated byte size=(\\d+)$");
    private static final Pattern MR_TIME_PATTERN = Pattern.compile("^.*Transferred \\d+(\\.\\d+)? (bytes|KB|MB|GB) in (\\d+(\\.\\d+)?) seconds.+$");
    private static final Pattern RECORD_NUM_PATTERN = Pattern.compile("^.*Retrieved (\\d+) records.+$");
    private static final Pattern MAP_PROGRESS_PATTERN = Pattern.compile("^.+map (\\d+)% reduce 0%$");
    private static final Pattern MAP_START_PATTERN = Pattern.compile("^.+map 0% reduce 0%$");
    private static final Pattern MAP_END_PATTERN = Pattern.compile("^.+map 100% reduce 0%$");
    private static final Pattern HERCULES_COUNTER_START = Pattern.compile("^\\s+Hercules Counters$");

    /**
     * 切分hercules运行过程，减少正则匹配的次数
     */
    private enum HerculesReaderStatus {
        INIT,
        MR_START,
        MR_END,
        HERCULES_COUNTER_START,
        HERCULES_COUNTER_END;
    }

    private class HerculesReader {
        private HerculesReaderStatus status = HerculesReaderStatus.INIT;
        private int lastProgress = -1;

        private Matcher match(Pattern pattern, String line) {
            Matcher matcher = pattern.matcher(line);
            return matcher.find() ? matcher : null;
        }

        public void readLine(String line) {
            Matcher matcher;
            long taskId = task.getId();
            try {
                // 注意，以下各个case设计两部分逻辑，状态转移与网络IO，如果这两者存在串行关系，一定把状态转移放在前面，管他IO成不成功，不能影响自动机运转
                switch (status) {
                    case INIT:
                        if ((matcher = match(MAP_NUM_PATTERN, line)) != null) {
                            int mapNum = Integer.parseInt(matcher.group(1));
                            replyService.setMapNum(taskId, mapNum);
                        } else if ((matcher = match(APPLICATION_ID_PATTERN, line)) != null) {
                            applicationId = matcher.group(1);
                            replyService.setApplicationId(taskId, applicationId);
                        } else if ((matcher = match(MR_LOG_URL_PATTERN, line)) != null) {
                            String url = matcher.group(1);
                            replyService.setMRLogUrl(taskId, url);
                        } else if (match(MAP_START_PATTERN, line) != null) {
                            status = HerculesReaderStatus.MR_START;
                        }
                        break;
                    case MR_START:
                        if (match(MAP_END_PATTERN, line) != null) {
                            status = HerculesReaderStatus.MR_END;
                        }
                        if ((matcher = match(MAP_PROGRESS_PATTERN, line)) != null) {
                            int progress = Integer.parseInt(matcher.group(1));
                            // 防止在10%之后又读到0%导致被重置，限制progress只能向上生长
                            if (progress > lastProgress) {
                                lastProgress = progress;
                                replyService.setMRProgress(taskId, progress);
                            }
                        }
                        break;
                    case MR_END:
                        if (match(HERCULES_COUNTER_START, line) != null) {
                            status = HerculesReaderStatus.HERCULES_COUNTER_START;
                        }
                        break;
                    case HERCULES_COUNTER_START:
                        if ((matcher = match(ESTIMATED_BYTE_SIZE_PATTERN, line)) != null) {
                            status = HerculesReaderStatus.HERCULES_COUNTER_END;
                            long byteSize = Long.parseLong(matcher.group(1));
                            replyService.setEstimatedByteSize(taskId, byteSize);
                        }
                        break;
                    case HERCULES_COUNTER_END:
                        if ((matcher = match(MR_TIME_PATTERN, line)) != null) {
                            BigDecimal mrTimeS = new BigDecimal(matcher.group(3));
                            long mrTimeMs = mrTimeS.multiply(new BigDecimal(1000))
                                    .setScale(0, RoundingMode.HALF_UP)
                                    .longValueExact();
                            replyService.setMRTimeMs(taskId, mrTimeMs);
                        } else if ((matcher = match(RECORD_NUM_PATTERN, line)) != null) {
                            long recordNum = Long.parseLong(matcher.group(1));
                            replyService.setRecordNum(taskId, recordNum);
                        }
                        break;
                    default:
                        throw new RuntimeException("Unknown HerculesReaderStatus: " + status);
                }
            } catch (Exception e) {
                getLog().warn(String.format("Error when parsing line [%s]", line), e);
            }
        }
    }

    public static class HerculesExecutorResult extends CommandExecutor.CommandExecutorResult {
        private Task task;

        public HerculesExecutorResult(CommandExecutor.CommandExecutorResult result, Task task) {
            super(result);
            this.task = task;
        }

        public Task getTask() {
            return task;
        }
    }
}
