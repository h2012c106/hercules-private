package com.xiaohongshu.db.node.service;

import com.xiaohongshu.db.share.entity.Node;
import com.xiaohongshu.db.share.entity.Task;
import org.springframework.core.io.Resource;

import java.io.InputStream;
import java.util.List;

public interface HerculesMissionService {

    public void submit(Task task,String jobName);

    public Exception kill(long taskId,String jobName);

    /**
     * @param taskList PENDING/RUNNING的task
     * @return
     */
    public List<Task> getZombie(List<Task> taskList);

}
