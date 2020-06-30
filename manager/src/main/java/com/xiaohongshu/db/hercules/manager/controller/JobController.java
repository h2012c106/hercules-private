package com.xiaohongshu.db.hercules.manager.controller;

import com.xiaohongshu.db.hercules.manager.service.JobService;
import com.xiaohongshu.db.share.entity.Cluster;
import com.xiaohongshu.db.share.entity.Job;
import com.xiaohongshu.db.share.entity.Task;
import com.xiaohongshu.db.share.utils.Constant;
import com.xiaohongshu.db.share.vo.JsonResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/job")
public class JobController {

    @Autowired
    private JobService jobService;

    @PostMapping("/insert")
    public long insert(@RequestBody @Validated Job job) {
        return jobService.insert(job);
    }

    @PatchMapping("/patch")
    public void patch(@RequestBody Map<String, Object> map) {
        if (!map.containsKey(Constant.ID_COL_NAME)) {
            throw new NullPointerException("Unfilled job id: " + map);
        }
        jobService.patch(map);
    }

    @DeleteMapping("/delete")
    public void delete(@RequestParam long id) {
        jobService.delete(id);
    }

    @GetMapping("/findOne")
    public Job findOne(Job job) {
        if (job.getId() == null) {
            throw new NullPointerException("Need parameter: 'id'.");
        }
        return jobService.findOne(job);
    }

    @GetMapping("/findAll")
    public List<Job> findAll() {
        return jobService.findAll();
    }

    /**
     * 仅返回命令，不跑
     *
     * @param jobId
     * @param templateMap
     * @return
     */
    @PostMapping("/tryTask")
    public JsonResult tryTask(@RequestParam long jobId, @RequestBody Map<String, Map<String, List<String>>> templateMap) {
        return new JsonResult(jobService.tryTask(
                jobId,
                templateMap.getOrDefault(Constant.SOURCE_PARAM_MAP_KEY, new HashMap<>(0)),
                templateMap.getOrDefault(Constant.TARGET_PARAM_MAP_KEY, new HashMap<>(0)),
                templateMap.getOrDefault(Constant.COMMON_PARAM_MAP_KEY, new HashMap<>(0)),
                templateMap.getOrDefault(Constant.D_PARAM_MAP_KEY, new HashMap<>(0))
        ));
    }

    @PostMapping("/startTask")
    public void startTask(@RequestParam long jobId, @RequestParam Cluster.Cloud sourceCloud, @RequestParam Cluster.Cloud targetCloud,
                          @RequestParam Task.Caller caller, @RequestBody Map<String, Map<String, List<String>>> templateMap) {
        jobService.startTask(
                jobId,
                sourceCloud,
                targetCloud,
                caller,
                templateMap.getOrDefault(Constant.SOURCE_PARAM_MAP_KEY, new HashMap<>(0)),
                templateMap.getOrDefault(Constant.TARGET_PARAM_MAP_KEY, new HashMap<>(0)),
                templateMap.getOrDefault(Constant.COMMON_PARAM_MAP_KEY, new HashMap<>(0)),
                templateMap.getOrDefault(Constant.D_PARAM_MAP_KEY, new HashMap<>(0))
        );
    }

}
