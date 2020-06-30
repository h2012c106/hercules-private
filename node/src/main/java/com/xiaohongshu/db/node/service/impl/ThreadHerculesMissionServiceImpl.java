package com.xiaohongshu.db.node.service.impl;

import com.xiaohongshu.db.node.service.HerculesMissionService;
import com.xiaohongshu.db.node.service.TaskStatusReplyService;
import com.xiaohongshu.db.node.service.YarnService;
import com.xiaohongshu.db.node.utils.HerculesExecutor;
import com.xiaohongshu.db.share.entity.Task;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

@Service
public class ThreadHerculesMissionServiceImpl implements HerculesMissionService, InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadHerculesMissionServiceImpl.class);

    private static final String HERCULES_COMMAND_THREAD_PREFIX = "HERCULES-EXECUTOR-THREAD-";

    @Autowired
    private TaskStatusReplyService replyService;

    @Autowired
    private YarnService yarnService;

    @Value("${hercules.node.mission.service.max.thread}")
    private int threadNum = 10;

    private final Map<Long, HerculesMission> missionMap = new ConcurrentHashMap<>();

    private ExecutorService herculesCommandPool;

    private final Object sbInsertZombieCheckLock = new Object();

    @Override
    public void afterPropertiesSet() throws Exception {
        // 存在并行上限，拥有无限等待队列，会自动释放线程的线程池
        herculesCommandPool = new ThreadPoolExecutor(
                0,
                threadNum,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new CustomizableThreadFactory(HERCULES_COMMAND_THREAD_PREFIX)
        ) {
            /**
             * 在线程执行完成后清除任务map中的记录
             * @param r
             * @param t
             */
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                Future<?> future;
                if (r instanceof Future<?>) {
                    future = (Future<?>) r;
                } else {
                    return;
                }
                if (!future.isCancelled()) {
                    try {
                        Object result = future.get();
                        if (result instanceof HerculesExecutor.HerculesExecutorResult) {
                            missionMap.remove(((HerculesExecutor.HerculesExecutorResult) result).getTask().getId());
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error("Literally, what can possibly went wrong when getting a done future.", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    @Override
    public void destroy() throws Exception {
        for (HerculesMission mission : missionMap.values()) {
            kill(mission);
        }
        herculesCommandPool.shutdown();
        herculesCommandPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void submit(Task task, String jobName) {
        Long taskId = null;
        try {
            HerculesMission mission = new HerculesMission(new HerculesExecutor(task, jobName, replyService, yarnService));
            // TODO 这里加了一把傻逼锁，目的是在于避免在insert行为结束后，put map行为开始前突然插入zombie检查，加锁能避免这两步之间突然插入zombie检查，保证检查时取到的map keys已经包括了数据库里置为RUNNING/PENDING的task了，其实要做的应该是submit之间不block，check zombie之间不block，这两者之间block
            synchronized (sbInsertZombieCheckLock) {
                // 插完数据库再真正起任务，避免万一系统崩溃，还能从数据库追溯出起了哪些任务
                taskId = replyService.insert(task);
                task.setId(taskId);
                missionMap.put(task.getId(), mission);
            }
            replyService.setSubmitTime(task.getId());
            // 这个放在mission map之后，先注册，后启动
            mission.setFuture(herculesCommandPool.submit(mission.getExecutor()));
        } catch (Exception e) {
            // 防止在insert流程后出错，task始终处于PENDING状态
            if (taskId != null) {
                try {
                    replyService.updateStatus(taskId, Task.TaskStatus.ERROR);
                } catch (Exception ee) {
                    LOG.error(ExceptionUtils.getStackTrace(ee));
                }
                throw new RuntimeException(String.format("Exception when submitting task[%d]: [%s]", taskId, task.getCommand()), e);
            } else {
                throw new RuntimeException(String.format("Exception when submitting task: [%s]", task.getCommand()), e);
            }
        }
    }

    private void kill(HerculesMission mission) throws IOException {
        mission.getExecutor().close();
        mission.getFuture().cancel(true);
    }

    private Exception killMRJob(String applicationId) {
        try {
            yarnService.kill(applicationId);
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    private List<String> findApplicationIdByJobName(String jobName) throws IOException {
        try {
            return yarnService.findApplicationIdByName(jobName);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * 记得同时关闭application
     *
     * @return
     */
    @Override
    public Exception kill(long taskId, String jobName) {
        HerculesMission mission = missionMap.remove(taskId);
        if (mission == null) {
            LOG.info(String.format("Dealing with a non-exist task killing (possibly zombie): %s[%d].", jobName, taskId));
            List<String> suspectList = Collections.emptyList();
            try {
                suspectList = findApplicationIdByJobName(jobName);
            } catch (IOException e) {
                return e;
            }
            if (suspectList.size() < 1) {
                return null;
            } else if (suspectList.size() > 1) {
                return new RuntimeException(String.format("The job [%s] has more than one mr task running: %d.", jobName, suspectList.size()));
            } else {
                String applicationId = suspectList.get(0);
                LOG.info(String.format("Find the application id of the non-exist task '%s[%d]': %s.", jobName, taskId, applicationId));
                return killMRJob(applicationId);
            }
        } else {
            try {
                // 由HerculesExecutor负责杀application
                kill(mission);
                return null;
            } catch (IOException e) {
                return new IOException(String.format("Kill mission [%d] failed.", taskId), e);
            }
        }
    }

    @Override
    public List<Task> getZombie(List<Task> taskList) {
        List<Task> res = new ArrayList<>(0);
        Set<Long> missionMapKeySet;
        synchronized (sbInsertZombieCheckLock) {
            missionMapKeySet = new HashSet<>(missionMap.keySet());
        }
        for (Task task : taskList) {
            // 不存在的话有两种情况:
            // 1. 置成PENDING/SUCCESS后node意外崩溃，重启后自然无此id
            // 2. manager正常，在service查PENDING/SUCCESS后，此task结束了数据库置status结束，且从map中移除，
            //    这种情况下，本函数会错误地返回之，但莫慌，返回service后会这么做——先将list内task置ZOMBIE状态，
            //    此时有个where条件，status = PENDING or RUNNING，这样就不会误置；
            //    之后service再从数据库再读一次ZOMBIE，这些就是真正的ZOMBIE。
            // 对于情况2，不用担心在置数据库时状态还未更新到SUCCESS/ERROR，因为如果从map中已经拿不到了，状态一定置好了。
            if (!missionMapKeySet.contains(task.getId())) {
                res.add(task);
            }
        }
        return res;
    }

    private static class HerculesMission {
        private HerculesExecutor executor;
        private Future future;

        public HerculesMission(HerculesExecutor executor) {
            this.executor = executor;
        }

        public void setFuture(Future future) {
            this.future = future;
        }

        public HerculesExecutor getExecutor() {
            return executor;
        }

        public Future getFuture() {
            return future;
        }
    }
}
