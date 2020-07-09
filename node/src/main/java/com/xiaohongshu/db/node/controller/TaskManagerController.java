package com.xiaohongshu.db.node.controller;

import com.xiaohongshu.db.node.service.HerculesMissionService;
import com.xiaohongshu.db.node.utils.CompressUtils;
import com.xiaohongshu.db.share.entity.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/rest/node/task")
public class TaskManagerController {

    @Autowired
    private HerculesMissionService herculesMissionService;

    @Value("${logging.file.path}")
    private String logDirPath;

    @PostMapping("/startTask")
    public void startTask(@RequestParam String jobName, @RequestBody Task task) {
        herculesMissionService.submit(task, jobName);
    }

    @PostMapping("/stopTask")
    public void stopTask(@RequestBody Map<String, Object> map) throws Exception {
        long taskId = new BigInteger(((List) map.get("taskId")).get(0).toString()).longValueExact();
        String jobName = (String) ((List) map.get("jobName")).get(0);
        Exception e = herculesMissionService.kill(taskId, jobName);
        if (e != null) {
            throw e;
        }
    }

    @PostMapping("/getZombie")
    public List<Task> getZombie(@RequestBody List<Task> taskList) {
        return herculesMissionService.getZombie(taskList);
    }

    @GetMapping("/getLog")
    public void getLog(@RequestParam long taskId, HttpServletRequest request, HttpServletResponse response) throws IOException {
        String fileUri = logDirPath + "/" + taskId;
        // 先检查一下文件在不在，不在也不用设置response属性了，免得调用方判断错误
        CompressUtils.getFile(fileUri);
        response.setCharacterEncoding(request.getCharacterEncoding());
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=" + taskId + ".zip");
        CompressUtils.zip(fileUri, response.getOutputStream());
        response.flushBuffer();
    }

    @GetMapping("/getMissionList")
    public List<Long> getMissionList() {
        return herculesMissionService.getMissionList();
    }

}
