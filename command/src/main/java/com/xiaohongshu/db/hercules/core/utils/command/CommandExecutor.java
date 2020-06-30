package com.xiaohongshu.db.hercules.core.utils.command;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.*;

public class CommandExecutor {

    private static final Log LOG = LogFactory.getLog(CommandExecutor.class);

    private static ExecutorService pool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            20L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

    public static CommandResult execute(String command, long timeout) {
        Process process = null;
        InputStream pIn = null;
        InputStream pErr = null;
        StreamGobbler outputGobbler = null;
        StreamGobbler errorGobbler = null;
        Future<Integer> executeFuture = null;
        try {
            LOG.info(command);
            process = Runtime.getRuntime().exec(command);
            final Process p = process;

            // close process's output stream.
            p.getOutputStream().close();

            pIn = process.getInputStream();
            outputGobbler = new StreamGobbler(pIn, "OUTPUT");
            outputGobbler.start();

            pErr = process.getErrorStream();
            errorGobbler = new StreamGobbler(pErr, "ERROR");
            errorGobbler.start();

            // create a Callable for the command's Process which can be called by an Executor
            Callable<Integer> call = new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    p.waitFor();
                    return p.exitValue();
                }
            };

            // submit the command's call and get the result from a
            executeFuture = pool.submit(call);
            int exitCode = executeFuture.get(timeout, TimeUnit.MILLISECONDS);
            return new CommandResult(exitCode, outputGobbler.getContent(), errorGobbler.getContent());
        } catch (IOException ex) {
            String errorMessage = "The command [" + command + "] execute failed.";
            LOG.error(errorMessage, ex);
            return new CommandResult(-1, null, null);
        } catch (TimeoutException ex) {
            String errorMessage = "The command [" + command + "] timed out.";
            LOG.error(errorMessage, ex);
            return new CommandResult(-1, null, null);
        } catch (ExecutionException ex) {
            String errorMessage = "The command [" + command + "] did not complete due to an execution error.";
            LOG.error(errorMessage, ex);
            return new CommandResult(-1, null, null);
        } catch (InterruptedException ex) {
            String errorMessage = "The command [" + command + "] did not complete due to an interrupted error.";
            LOG.error(errorMessage, ex);
            return new CommandResult(-1, null, null);
        } finally {
            if (executeFuture != null) {
                try {
                    executeFuture.cancel(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (pIn != null) {
                closeQuietly(pIn);
                if (outputGobbler != null && !outputGobbler.isInterrupted()) {
                    outputGobbler.interrupt();
                }
            }
            if (pErr != null) {
                closeQuietly(pErr);
                if (errorGobbler != null && !errorGobbler.isInterrupted()) {
                    errorGobbler.interrupt();
                }
            }
            if (process != null) {
                process.destroy();
            }
        }
    }

    private static void closeQuietly(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (IOException e) {
            LOG.error("exception", e);
        }
    }
}