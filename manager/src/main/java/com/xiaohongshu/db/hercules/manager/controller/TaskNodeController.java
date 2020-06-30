package com.xiaohongshu.db.hercules.manager.controller;

import com.xiaohongshu.db.hercules.manager.service.TaskService;
import com.xiaohongshu.db.share.entity.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/rest/manager/task")
public class TaskNodeController {

    @Autowired
    private TaskService taskService;

    @PostMapping("/insert")
    public long insert(@RequestBody @Validated Task task) {
        return taskService.insert(task);
    }

    @GetMapping("/setStartTime")
    public void setStartTime(@RequestParam long taskId, @RequestParam long startTime) {
        taskService.setStartTime(taskId, startTime);
    }

    @GetMapping("/setSubmitTime")
    public void setSubmitTime(@RequestParam long taskId, @RequestParam long submitTime) {
        taskService.setSubmitTime(taskId, submitTime);
    }

    @GetMapping("/setTotalTimeMs")
    public void setTotalTimeMs(@RequestParam long taskId, @RequestParam long totalTimeMs) {
        taskService.setTotalTimeMs(taskId, totalTimeMs);
    }

    @GetMapping("/updateStatus")
    public void updateStatus(@RequestParam long taskId, @RequestParam Task.TaskStatus taskStatus) {
        taskService.updateStatus(taskId, taskStatus);
    }

    @GetMapping("/setMapNum")
    public void setMapNum(@RequestParam long taskId, @RequestParam int mapNum) {
        taskService.setMapNum(taskId, mapNum);
    }

    @GetMapping("/setApplicationId")
    public void setApplicationId(@RequestParam long taskId, @RequestParam String applicationId) {
        taskService.setApplicationId(taskId, applicationId);
    }

    @GetMapping("/setMRLogUrl")
    public void setMRLogUrl(@RequestParam long taskId, @RequestParam String mrLogUrl) {
        taskService.setMRLogUrl(taskId, mrLogUrl);
    }

    @GetMapping("/setMRProgress")
    public void setMRProgress(@RequestParam long taskId, @RequestParam int mrProgress) {
        taskService.setMRProgress(taskId, mrProgress);
    }

    @GetMapping("/setEstimatedByteSize")
    public void setEstimatedByteSize(@RequestParam long taskId, @RequestParam long byteSize) {
        taskService.setEstimatedByteSize(taskId, byteSize);
    }

    @GetMapping("/setMRTimeMs")
    public void setMRTimeMs(@RequestParam long taskId, @RequestParam long mrTimeMs) {
        taskService.setMRTimeMs(taskId, mrTimeMs);
    }

    @GetMapping("/setRecordNum")
    public void setRecordNum(@RequestParam long taskId, @RequestParam long recordNum) {
        taskService.setRecordNum(taskId, recordNum);
    }

    @GetMapping("/end")
    public void end(@RequestParam long taskId, @RequestParam Task.TaskStatus status) {
        taskService.end(taskId, status);
    }

}
