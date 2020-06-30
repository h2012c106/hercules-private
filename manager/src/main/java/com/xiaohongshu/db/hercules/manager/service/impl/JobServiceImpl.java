package com.xiaohongshu.db.hercules.manager.service.impl;

import com.xiaohongshu.db.hercules.manager.dao.mapper.ClusterMapper;
import com.xiaohongshu.db.hercules.manager.dao.mapper.JobMapper;
import com.xiaohongshu.db.hercules.manager.dao.mapper.NodeMapper;
import com.xiaohongshu.db.hercules.manager.exception.NotFoundException;
import com.xiaohongshu.db.hercules.manager.service.ClusterService;
import com.xiaohongshu.db.hercules.manager.service.JobService;
import com.xiaohongshu.db.hercules.manager.utils.CommandGenerator;
import com.xiaohongshu.db.share.entity.Cluster;
import com.xiaohongshu.db.share.entity.Job;
import com.xiaohongshu.db.share.entity.Node;
import com.xiaohongshu.db.share.entity.Task;
import com.xiaohongshu.db.share.utils.Constant;
import com.xiaohongshu.db.share.utils.RestUtils;
import com.xiaohongshu.db.share.vo.JsonResult;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class JobServiceImpl implements JobService {

    @Autowired
    private JobMapper jobMapper;

    @Autowired
    private NodeMapper nodeMapper;

    @Autowired
    private ClusterMapper clusterMapper;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public long insert(Job entity) {
        jobMapper.insert(entity);
        return entity.getId();
    }

    @Override
    public void delete(long id) {
        jobMapper.delete(id);
    }

    @Override
    public Job findOne(Job entity) {
        Job res = jobMapper.findOne(entity);
        if (res == null) {
            throw new NotFoundException("No job with job info: " + entity);
        } else {
            return res;
        }
    }

    @Override
    public List<Job> findAll() {
        return jobMapper.findAll();
    }

    @Override
    public void patch(Map<String, Object> map) {
        int row = jobMapper.patch(map);
        if (row != 1) {
            throw new RuntimeException("Unexpected updated row: " + row);
        }

    }

    @Override
    public String tryTask(long jobId, Map<String, List<String>> sourceTemplateMap, Map<String, List<String>> targetTemplateMap, Map<String, List<String>> commonTemplateMap, Map<String, List<String>> dTemplateMap) {
        // 生成command并检查参数
        Job job = new Job(jobId);
        job = findOne(job);
        return CommandGenerator.generateHerculesCommand(job, sourceTemplateMap, targetTemplateMap, commonTemplateMap, dTemplateMap);
    }

    /**
     * 由于这里在向Node发送请求后并未真正往数据库插入一条task，直到node反调insert时才创建，且insert前
     *
     * @param jobId
     * @param sourceTemplateMap
     * @param targetTemplateMap
     * @param commonTemplateMap
     * @param dTemplateMap
     * @return
     */
    @Override
    public void startTask(long jobId, Cluster.Cloud sourceCloud, Cluster.Cloud targetCloud, Task.Caller caller,
                          Map<String, List<String>> sourceTemplateMap, Map<String, List<String>> targetTemplateMap,
                          Map<String, List<String>> commonTemplateMap, Map<String, List<String>> dTemplateMap) {
        String command = tryTask(jobId, sourceTemplateMap, targetTemplateMap, commonTemplateMap, dTemplateMap);

        String cloudCode;
        if (sourceCloud == null && targetCloud == null) {
            throw new RuntimeException("Need at least one cloud information.");
        } else {
            // 优先用target的
            cloudCode = targetCloud == null ? sourceCloud.name() : targetCloud.name();
        }
        Cluster cluster = clusterMapper.pickByCloud(cloudCode);
        if (cluster == null) {
            throw new RuntimeException("Cannot find cluster with cloud: " + cloudCode);
        }
        List<Node> nodeList = nodeMapper.findByClusterId(cluster.getId());
        nodeList = nodeList.stream().filter(node -> node.getStatus() == Node.NodeStatus.ON).collect(Collectors.toList());
        if (nodeList.size() == 0) {
            throw new RuntimeException("No living node in cluster: " + cluster.getName());
        }
        Node randomedNode = nodeList.get(RandomUtils.nextInt(0, nodeList.size()));

        // 申请运行此job，若置失败则说明已有运行中任务，报错
        int affectedRow = jobMapper.readyTask(jobId);
        if (affectedRow == 0) {
            throw new RuntimeException(String.format("The [%s] is running.", jobId));
        }

        // 手动回滚记录，避免事务嵌套
        try {
            Task newTask = new Task();
            newTask.setJobId(jobId);
            newTask.setNodeId(randomedNode.getId());
            newTask.setCaller(caller);
            newTask.setCommand(command);

            // 向node发送启动任务请求，在node侧会回调manager更新job task信息，若失败，会回滚，不会始终把job的taskId置着-1
            String url = UriComponentsBuilder
                    .fromHttpUrl(Constant.nodeRestBaseUrl(randomedNode.getHost(), randomedNode.getPort()))
                    .pathSegment("task")
                    .pathSegment("startTask")
                    .queryParam("jobName", findOne(new Job(jobId)).getName())
                    .build()
                    .encode()
                    .toString();
            MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
            header.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
            header.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            HttpEntity<Task> httpEntity = new HttpEntity<>(newTask, header);
            ResponseEntity<JsonResult> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, JsonResult.class);
            RestUtils.validateResponseEntity(responseEntity);
        } catch (Exception e) {
            jobMapper.unreadyTask(jobId);
            throw e;
        }
    }

}
