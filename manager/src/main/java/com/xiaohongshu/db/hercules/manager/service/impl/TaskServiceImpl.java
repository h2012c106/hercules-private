package com.xiaohongshu.db.hercules.manager.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaohongshu.db.hercules.manager.dao.mapper.JobMapper;
import com.xiaohongshu.db.hercules.manager.dao.mapper.TaskMapper;
import com.xiaohongshu.db.hercules.manager.exception.NotFoundException;
import com.xiaohongshu.db.hercules.manager.service.JobService;
import com.xiaohongshu.db.hercules.manager.service.NodeService;
import com.xiaohongshu.db.hercules.manager.service.TaskService;
import com.xiaohongshu.db.share.entity.Job;
import com.xiaohongshu.db.share.entity.Node;
import com.xiaohongshu.db.share.entity.Task;
import com.xiaohongshu.db.share.utils.Constant;
import com.xiaohongshu.db.share.utils.RestUtils;
import com.xiaohongshu.db.share.vo.JsonResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class TaskServiceImpl implements TaskService {

    @Autowired
    private TaskMapper taskMapper;

    @Autowired
    private JobMapper jobMapper;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private JobService jobService;

    @Autowired
    private RestTemplate restTemplate;

    private void patch(long taskId, String propertyName, Object value) {
        Map<String, Object> patchMap = new HashMap<>();
        patchMap.put(Constant.ID_COL_NAME, taskId);
        patchMap.put(propertyName, value);
        taskMapper.patch(patchMap);
    }

    @Override
    public void setStartTime(long taskId, long startTime) {
        patch(taskId, "startTime", new Timestamp(startTime));
    }

    @Override
    public void setSubmitTime(long taskId, long submitTime) {
        patch(taskId, "submitTime", new Timestamp(submitTime));
    }

    @Override
    public void setTotalTimeMs(long taskId, long totalTimeMs) {
        patch(taskId, "totalTimeMs", totalTimeMs);
    }

    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class)
    public void updateStatus(long taskId, Task.TaskStatus status) {
        taskMapper.updateStatus(taskId, status);
        // 修改状态为结束，则修改job状态为空闲
        if (status.isEnd()) {
            // 这里也可以给rds ACK但是好像显得太重了
            jobMapper.stopTask(taskId);
        }
    }

    @Override
    public void setMapNum(long taskId, int mapNum) {
        patch(taskId, "mapNum", mapNum);
    }

    @Override
    public void setApplicationId(long taskId, String applicationId) {
        patch(taskId, "applicationId", applicationId);
    }

    @Override
    public void setMRLogUrl(long taskId, String url) {
        patch(taskId, "mrLogUrl", url);
    }

    @Override
    public void setMRProgress(long taskId, int progress) {
        patch(taskId, "mrProgress", progress);
    }

    @Override
    public void setEstimatedByteSize(long taskId, long byteSize) {
        patch(taskId, "estimatedByteSize", byteSize);
    }

    @Override
    public void setMRTimeMs(long taskId, long ms) {
        patch(taskId, "mrTimeMs", ms);
    }

    @Override
    public void setRecordNum(long taskId, long recordNum) {
        patch(taskId, "recordNum", recordNum);
    }

    /**
     * 保证不发生脏读，即在没有update job的时候就读到新insert进去的task
     *
     * @param task
     * @return
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class)
    public long insert(Task task) {
        taskMapper.insert(task);
        jobMapper.startTask(task.getJobId(), task.getId());
        return task.getId();
    }

    @Override
    public void delete(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Task findOne(Task entity) {
        Task res = taskMapper.findOne(entity);
        if (res == null) {
            throw new NotFoundException("No task with task info: " + entity);
        } else {
            return res;
        }
    }

    @Override
    public List<Task> findAll() {
        return taskMapper.findAll();
    }

    @Override
    public void patch(Map<String, Object> map) {
        throw new UnsupportedOperationException();
    }

    /**
     * 对置zombie的操作若有误置的疑虑可以看node中ThreadHerculesMissionServiceImpl#getZombie方法的注释
     *
     * @return
     */
    @Override
    public List<Task> findZombies() {
        List<Task> res = new LinkedList<>();
        for (Node node : nodeService.findAll()) {
            List<Task> nodeRunningTaskList = taskMapper.findByNodeId(node.getId())
                    .stream().filter(n -> n.getStatus().isRunning())
                    .collect(Collectors.toList());
            try {
                String url = UriComponentsBuilder
                        .fromHttpUrl(Constant.nodeRestBaseUrl(node.getHost(), node.getPort()))
                        .pathSegment("task")
                        .pathSegment("getZombie")
                        .build()
                        .encode()
                        .toString();
                MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
                header.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
                header.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
                HttpEntity<List<Task>> httpEntity = new HttpEntity<>(nodeRunningTaskList, header);
                ResponseEntity<JsonResult> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, JsonResult.class);
                RestUtils.validateResponseEntity(responseEntity);
                res.addAll((List<Task>) RestUtils.parseResponseEntity(responseEntity));
            } catch (Exception ignore) {
            }
        }
        return res;
    }

    /**
     * 由于kill/success/error都会置标志位，而本函数只负责发起kill，故标志位的恢复由逻辑更为集中的updateTaskStatus发起，manager不发起
     */
    @Override
    public void stopTask(long taskId) {
        // 理论上因为insert task和update job是事务，不可能出现，但是这一步能够完全保证能正确的发起kill该任务
        if (jobMapper.findByRunningTaskId(taskId) == null) {
            throw new RuntimeException(String.format("The task [%s] is not registered to job.", taskId));
        }
        Task task = findOne(new Task(taskId));
        Node node = nodeService.findOne(new Node(task.getNodeId()));
        Job job = jobService.findOne(new Job(task.getJobId()));

        String url = UriComponentsBuilder
                .fromHttpUrl(Constant.nodeRestBaseUrl(node.getHost(), node.getPort()))
                .pathSegment("task")
                .pathSegment("stopTask")
                .build()
                .encode()
                .toString();
        MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
        header.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        header.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        MultiValueMap<String, Object> value = new LinkedMultiValueMap<>();
        value.add("taskId", taskId);
        value.add("jobName", job.getName());
        HttpEntity<MultiValueMap<String, Object>> httpEntity = new HttpEntity<>(value, header);
        ResponseEntity<JsonResult> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void stopTasks(List<Long> idList) {
        Map<Long, Exception> exceptionMap = new LinkedHashMap<>();
        for (long id : idList) {
            try {
                stopTask(id);
            } catch (Exception e) {
                exceptionMap.put(id, e);
            }
        }
        if (exceptionMap.size() > 0) {
            throw new RuntimeException("Following task(s) kill failed: " + exceptionMap);
        }
    }

    @Override
    public InputStream getLog(long jobId, long taskId) {
        Task task = findOne(new Task(taskId));
        Node node = nodeService.findOne(new Node(task.getNodeId()));

        String url = UriComponentsBuilder.fromHttpUrl(Constant.nodeRestBaseUrl(node.getHost(), node.getPort()))
                .pathSegment("task")
                .pathSegment("getLog")
                .queryParam("taskId", taskId)
                .build()
                .encode()
                .toString();
        ResponseEntity<Resource> responseEntity = restTemplate.getForEntity(url, Resource.class);
        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            try {
                if ("zip".equals(responseEntity.getHeaders().getContentType().getSubtype())) {
                    return responseEntity.getBody().getInputStream();
                } else {
                    ByteArrayResource byteArrayResource = (ByteArrayResource) responseEntity.getBody();
                    String s = new String(byteArrayResource.getByteArray());
                    JsonResult jsonResult = new ObjectMapper().readValue(s, JsonResult.class);
                    throw new RuntimeException("Get file error: " + jsonResult.getErrorStack());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException();
        }
    }

    @Override
    public Task findByTaskId(long id) {
        return taskMapper.findOne(new Task(id));
    }

    @Override
    public List<Task> findByJobId(long id) {
        return taskMapper.findByJobId(id);
    }

    @Override
    public Task findLatestByJobId(long id) {
        return taskMapper.findLatestByJobId(id);
    }

    @Override
    public void end(long id, Task.TaskStatus status) {
        Task task = findOne(new Task(id));
        if (task.getCaller() == Task.Caller.RDS) {

        }
    }
}
