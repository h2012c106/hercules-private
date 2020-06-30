package com.xiaohongshu.db.hercules.manager.service;

import com.xiaohongshu.db.share.entity.Task;

import java.io.InputStream;
import java.util.List;

public interface TaskService extends BaseService<Task> {

    public void setStartTime(long taskId, long startTime);

    public void setSubmitTime(long taskId, long submitTime);

    public void setTotalTimeMs(long taskId, long totalTimeMs);

    public void updateStatus(long taskId, Task.TaskStatus status);

    public void setMapNum(long taskId, int mapNum);

    public void setApplicationId(long taskId, String applicationId);

    public void setMRLogUrl(long taskId, String url);

    public void setMRProgress(long taskId, int progress);

    public void setEstimatedByteSize(long taskId, long byteSize);

    public void setMRTimeMs(long taskId, long ms);

    public void setRecordNum(long taskId, long recordNum);

    public long insert(Task task);

    public List<Task> findZombies();

    public void stopTask(long id);

    public void stopTasks(List<Long> idList);

    public InputStream getLog(long jobId, long taskId);

    public Task findByTaskId(long id);

    public List<Task> findByJobId(long id);

    public Task findLatestByJobId(long id);

    public void end(long id, Task.TaskStatus status);

}
