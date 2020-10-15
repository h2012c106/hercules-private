package com.xiaohongshu.db.hercules.core.mr.output;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesCounter;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesStatus;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 接受一批缓存好的数据异步多线程写的范式
 *
 * @param <T> 线程任务需要的参数
 */
public abstract class MultiThreadAsyncWriter<T, M extends MultiThreadAsyncWriter.WorkerMission> {

    private static final Log LOG = LogFactory.getLog(MultiThreadAsyncWriter.class);

    private int threadNum;
    private ExecutorService threadPool = null;
    final private BlockingQueue<M> missionQueue = new SynchronousQueue<M>();
    final private List<Exception> exceptionList = new ArrayList<Exception>();
    final private AtomicBoolean threadPoolClosed = new AtomicBoolean(false);

    public MultiThreadAsyncWriter(int threadNum) {
        this.threadNum = threadNum;
    }

    /**
     * 初始化线程无限循环任务中的环境变量
     * 实际是单个线程专用，无需考虑线程安全问题
     *
     * @return
     * @throws Exception
     */
    abstract protected T initializeThreadContext() throws Exception;

    /**
     * 执行写行为
     *
     * @param context
     * @throws Exception
     */
    abstract protected void doWrite(T context, M mission) throws Exception;

    /**
     * {@link #doWrite(Object, WorkerMission)}阶段出错的处理
     *
     * @param context
     * @param mission
     */
    abstract protected void handleException(T context, M mission, Exception e);

    /**
     * 若环境变量有生命周期，关闭之
     *
     * @param context
     */
    abstract protected void closeContext(T context);

    public final void run() throws Exception {
        if (threadPool != null) {
            return;
        }
        threadPool = new ThreadPoolExecutor(threadNum,
                this.threadNum,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("Hercules-Export-Worker-%d").build(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        for (int i = 0; i < threadNum; ++i) {
            threadPool.execute(new Runnable() {
                final T context = initializeThreadContext();

                @Override
                public void run() {
                    LOG.info(String.format("Thread %s start.", Thread.currentThread().getName()));
                    while (true) {
                        // 从任务队列里阻塞取
                        M mission = null;
                        try {
                            long startTime = System.currentTimeMillis();
                            mission = missionQueue.take();
                            HerculesStatus.add(null, HerculesCounter.ASYNC_WRITER_TAKE_TIME, System.currentTimeMillis() - startTime);
                        } catch (InterruptedException e) {
                            LOG.warn("Worker's taking mission interrupted: " + ExceptionUtils.getStackTrace(e));
                            continue;
                        }

                        if (mission == null) {
                            LOG.warn("Null mission");
                            continue;
                        }

                        // try...catch原因在于不能因为一次循环里的错误就让循环断掉，必须得留着循环，其实是必须留着take方法
                        // 考虑这么一种情况，在close时最后一次execUpdate崩了，如果出循环了那么主线程停止命令将永远阻塞在put上，
                        // 或者这种情况，在一次execUpdate头部check未检查出错误，但是在主线程阻塞提交任务时，
                        // n个线程全崩了且跳出循环了，此时无人take，死锁
                        try {
                            doWrite(context, mission);
                        } catch (Exception e) {
                            exceptionList.add(e);
                            handleException(context, mission, e);
                        }

                        if (mission.needClose()) {
                            break;
                        }
                    }
                    try {
                        closeContext(context);
                    } catch (Exception e) {
                        LOG.warn(String.format("Thread %s close with exception: %s",
                                Thread.currentThread().getName(), ExceptionUtils.getStackTrace(e)));
                    }
                    LOG.info(String.format("Thread %s use %s for taking mission.",
                            Thread.currentThread().getName(), HerculesStatus.getStrValue(HerculesCounter.ASYNC_WRITER_TAKE_TIME)));
                }
            });
        }
    }

    /**
     * 提交任务
     *
     * @param mission
     * @throws InterruptedException
     */
    public final void put(M mission) throws InterruptedException, IOException {
        // 先检查有没有抛错
        checkException(true);
        long startTime = System.currentTimeMillis();
        missionQueue.put(mission);
        HerculesStatus.add(null, HerculesCounter.ASYNC_WRITER_PUT_TIME, System.currentTimeMillis() - startTime);
    }

    abstract protected M innerGetCloseMission();

    private M getCloseMission() {
        M result = innerGetCloseMission();
        if (!result.needClose()) {
            throw new RuntimeException("Why the close mission doesn't need close.");
        }
        return result;
    }

    private void close() throws InterruptedException {
        if (!threadPoolClosed.getAndSet(true) && threadPool != null) {
            // 起了多少个线程就发多少个停止命令，在worker逻辑中已经保证了错误不会导致不再take，且threadPoolClosed保证此逻辑只会走一次
            for (int i = 0; i < threadNum; ++i) {
                missionQueue.put(getCloseMission());
            }
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            threadPool = null;
        }
    }

    private void checkException(boolean needClose) throws IOException, InterruptedException {
        if (exceptionList.size() > 0) {
            // just for elegance，直接抛也可以
            if (needClose) {
                close();
            }
            LOG.error(ExceptionUtils.getStackTrace(exceptionList.get(0)));
            throw new IOException(exceptionList.get(0));
        }
    }

    public final void done() throws IOException, InterruptedException {
        close();
        LOG.info(String.format("Use %s for putting mission.", HerculesStatus.getStrValue(HerculesCounter.ASYNC_WRITER_PUT_TIME)));
        // 尘埃落定了再检查有没有抛错
        checkException(false);
    }

    public static class WorkerMission {
        private boolean close;

        public WorkerMission(boolean close) {
            this.close = close;
        }

        public boolean needClose() {
            return close;
        }

        public void setClose(boolean close) {
            this.close = close;
        }
    }

}
