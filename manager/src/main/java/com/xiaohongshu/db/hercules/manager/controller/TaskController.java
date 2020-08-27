package com.xiaohongshu.db.hercules.manager.controller;

import com.xiaohongshu.db.hercules.manager.service.TaskService;
import com.xiaohongshu.db.share.entity.Task;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@RestController
@RequestMapping(value = "/task")
public class TaskController {

    @Autowired
    private TaskService taskService;

    @GetMapping("/findZombies")
    public List<Task> findZombies() {
        return taskService.findZombies();
    }

    @GetMapping("/stopTask")
    public void stopTask(@RequestParam long id) {
        taskService.stopTask(id);
    }

    @GetMapping("/stopTasks")
    public void stopTasks(@RequestParam List<Long> idList) {
        taskService.stopTasks(idList);
    }

    @GetMapping("/getLog")
    public void getLog(@RequestParam long id, HttpServletRequest request, HttpServletResponse response) throws IOException {
        // 先取一下，免得先把response属性设置好了后再出错返回json导致，前端还以为返回的文件
        InputStream zipInputStream = taskService.getLog(-1, id);
        response.setCharacterEncoding(request.getCharacterEncoding());
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=" + id + ".zip");
        IOUtils.copy(zipInputStream, response.getOutputStream());
        response.flushBuffer();
    }

    @GetMapping("/findByTaskId")
    public Task findByTaskId(@RequestParam long id) {
        return taskService.findByTaskId(id);
    }

    @GetMapping("/findByJobId")
    public List<Task> findByJobId(@RequestParam long id) {
        return taskService.findByJobId(id);
    }

    @GetMapping("/findLatestByJobId")
    public Task findLatestByJobId(@RequestParam long id) {
        return taskService.findLatestByJobId(id);
    }

}
