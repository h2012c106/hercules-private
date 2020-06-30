package com.xiaohongshu.db.node.service;

import com.xiaohongshu.db.share.entity.Task;
import org.springframework.beans.factory.annotation.Value;

public interface TaskStatusReplyService {

    public void setStartTime(long taskId);

    public void setSubmitTime(long taskId);

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

    public void end(long taskId, Task.TaskStatus status);

}
