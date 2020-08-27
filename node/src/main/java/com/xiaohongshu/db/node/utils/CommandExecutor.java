package com.xiaohongshu.db.node.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CommandExecutor implements Callable<CommandExecutor.CommandExecutorResult>, Closeable {

    protected String[] command;
    private Process process = null;

    private InputStream inputStream = null;
    private InputStreamReader inputStreamReader = null;
    private BufferedReader bufferedReader = null;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean readerReleased = new AtomicBoolean(false);
    private final AtomicBoolean doneHook = new AtomicBoolean(false);

    /**
     * 在释放资源同时加锁，限制call的线程不要同时进行赋值操作，防止kill线程先进到if null的判断后call线程赋值，造成我以为我kill了实际没有的结果
     * 虽然两个锁但是不用担心死锁，call线程两个锁是串行且锁内无任何阻塞操作，必会释放锁，kill线程之间并行没问题，拿锁顺序是一致的，不会死锁
     */
    private final Object closeLock = new Object();
    private final Object readerReleaseLock = new Object();

    /**
     * 控制是否清除子进程，默认为打开，但是清除子进程的进程不能再cleanup，否则会无限套娃，况且kill -9或ps命令以计算为主不吃IO，不可能长时间占有系统资源
     */
    private final boolean cleanup;

    public CommandExecutor(String command) {
        this(command, true);
    }

    /**
     * 只有自己允许随意置cleanup，因为我知道我的命令生命周期极短，不吃系统资源，外部调用的命令行必须cleanup，谨防僵尸产生
     * @param command
     * @param cleanup
     */
    private CommandExecutor(String command, boolean cleanup) {
        this.command = new String[]{"/bin/bash", "-c", "source /etc/profile > /dev/null && ( " + command + " 2>&1 )"};
        this.cleanup = cleanup;
    }

    protected Logger getLog() {
        return LoggerFactory.getLogger(CommandExecutor.class);
    }

    @Override
    public CommandExecutorResult call() {
        int exitCode = -1;
        StringBuilder sb = new StringBuilder();
        String commandStr = StringUtils.join(command, " ");
        try {
            getLog().info("Execute command: " + commandStr);
            // 防止出现close线程判断完非空再进这个逻辑
            synchronized (closeLock) {
                if (!closed.get()) {
                    process = Runtime.getRuntime().exec(command);
                } else {
                    throw new InterruptedException("Interrupted when initialize process.");
                }
            }
            process.getOutputStream().close();
            process.getErrorStream().close();

            try {
                synchronized (readerReleaseLock) {
                    if (!readerReleased.get()) {
                        inputStream = process.getInputStream();
                        inputStreamReader = new InputStreamReader(inputStream);
                        bufferedReader = new BufferedReader(inputStreamReader);
                    } else {
                        throw new InterruptedException("Interrupted when initialize inputStream and reader.");
                    }
                }
                String line;
                while (!readerReleased.get() && (line = bufferedReader.readLine()) != null) {
                    sb.append(line).append("\n");
                    getLog().info(line);
                    readLine(line);
                }
            } catch (IOException ex) {
                getLog().warn("Failed to read process output.", ex);
            } finally {
                releaseReader();
            }

            exitCode = process.waitFor();

            if (exitCode == 0) {
                getLog().info(String.format("Command [%s] execute done and successfully.", commandStr));
            } else {
                getLog().error(String.format("Command [%s] execute done and fail, return code: %d", commandStr, exitCode));
            }
        } catch (Exception ex) {
            String errorMessage = "The command [" + commandStr + "] execute failed.";
            getLog().error(errorMessage, ex);
        } finally {
            release();
            afterDone(exitCode);
        }
        return new CommandExecutorResult(exitCode, sb.toString());
    }

    protected void readLine(String line) {
    }

    protected void innerAfterDone(int exitCode) {
    }

    private void afterDone(int exitCode) {
        if (!doneHook.getAndSet(true)) {
            innerAfterDone(exitCode);
        }
    }

    protected void innerAfterKilled() throws IOException {
    }

    private void afterKilled() throws IOException {
        if (!doneHook.getAndSet(true)) {
            innerAfterKilled();
        }
    }

    private void releaseReader() {
        synchronized (readerReleaseLock) {
            if (!readerReleased.getAndSet(true)) {
                getLog().info("Closing process input stream...");
                if (inputStream != null) {
                    closeQuietly(inputStream);
                }
                if (inputStreamReader != null) {
                    try {
                        inputStreamReader.close();
                    } catch (IOException ignore) {
                    }
                }
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException ignore) {
                    }
                }
            }
        }
    }

    private synchronized int getPid(Process p) {
        int pid = -1;
        try {
            if ("java.lang.UNIXProcess".equals(p.getClass().getName())) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getInt(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }

    private Set<Integer> closeProcess(int pid, boolean killSelf) {
        Set<Integer> failedSet = new HashSet<>();
        try {
            CommandExecutor commandExecutor = new CommandExecutor(String.format("ps -ef | awk '$3 == %d {print $2}'", pid), false);
            CommandExecutorResult result = commandExecutor.call();
            List<Integer> pidList = Arrays.stream(result.getMessage().split("\\s+"))
                    .map(String::trim)
                    .filter(item -> item.length() > 0)
                    .map(item -> new BigInteger(item).intValueExact())
                    .collect(Collectors.toList());
            getLog().info(String.format("Thread %d has sub pid(s): %s", pid, pidList.toString()));
            // 先把子进程们关了
            for (Integer i : pidList) {
                failedSet.addAll(closeProcess(i, true));
            }
            if (killSelf) {
                CommandExecutorResult killSelfResult = new CommandExecutor(String.format("kill -9 %d", pid), false).call();
                // 如果这个进程已经因为子进程死亡自己挂了，那么No such process很正常。这个函数已经很魔幻了，不差这一个魔法值
                if (killSelfResult.getExitCode() != 0
                        && !StringUtils.contains(killSelfResult.getMessage(), "No such process")) {
                    failedSet.add(pid);
                } else {
                    getLog().info(String.format("Process [%d] close successfully.", pid));
                }
            }
            return failedSet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanupSubProcess() {
        int pid = getPid(process);
        if (pid != -1) {
            Set<Integer> failedSet = closeProcess(pid, false);
            if (failedSet.size() > 0) {
                getLog().error("The following child pid is not killed successfully: " + failedSet);
            }
        } else {
            getLog().error("Cannot get process pid, fail to close sub process.");
        }
    }

    private void release() {
        synchronized (closeLock) {
            if (!closed.getAndSet(true)) {
                releaseReader();
                if (process != null) {
                    getLog().info("Killing command process...");
                    if (cleanup) {
                        cleanupSubProcess();
                    }
                    process.destroy();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        release();
        afterKilled();
    }

    private void closeQuietly(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (IOException e) {
            getLog().error("Closing exception", e);
        }
    }

    public static class CommandExecutorResult {
        private int exitCode;
        private String message;

        public CommandExecutorResult(CommandExecutorResult result) {
            this.exitCode = result.getExitCode();
            this.message = result.getMessage();
        }

        public CommandExecutorResult(int exitCode, String message) {
            this.exitCode = exitCode;
            this.message = message;
        }

        public int getExitCode() {
            return exitCode;
        }

        public String getMessage() {
            return message;
        }
    }
}
