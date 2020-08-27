package com.xiaohongshu.db.hercules.manager.service;

import com.xiaohongshu.db.share.entity.Cluster;
import com.xiaohongshu.db.share.entity.Job;
import com.xiaohongshu.db.share.entity.Task;

import java.util.List;
import java.util.Map;

public interface JobService extends BaseService<Job> {

    public String tryTask(long jobId,
                          Map<String, List<String>> sourceTemplateMap, Map<String, List<String>> targetTemplateMap,
                          Map<String, List<String>> commonTemplateMap, Map<String, List<String>> dTemplateMap);

    public void startTask(long jobId, Cluster.Cloud sourceCloud, Cluster.Cloud targetCloud, Task.Caller caller,
                          Map<String, List<String>> sourceTemplateMap, Map<String, List<String>> targetTemplateMap,
                          Map<String, List<String>> commonTemplateMap, Map<String, List<String>> dTemplateMap);

}
