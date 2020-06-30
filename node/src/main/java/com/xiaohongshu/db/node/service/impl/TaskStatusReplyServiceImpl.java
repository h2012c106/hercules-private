package com.xiaohongshu.db.node.service.impl;

import com.xiaohongshu.db.node.service.TaskStatusReplyService;
import com.xiaohongshu.db.share.utils.RestUtils;
import com.xiaohongshu.db.share.entity.Task;
import com.xiaohongshu.db.share.utils.Constant;
import com.xiaohongshu.db.share.vo.JsonResult;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.math.BigInteger;

/**
 * TODO setXXX方法可以改成异步调用
 */
@Service
public class TaskStatusReplyServiceImpl implements TaskStatusReplyService, InitializingBean {

    @Value("${hercules.node.manager.service.host}")
    private String managerHost;
    @Value("${hercules.node.manager.service.port}")
    private long managerPort;
    private String managerBaseUrl;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public void afterPropertiesSet() throws Exception {
        managerBaseUrl = Constant.managerRestBaseUrl(managerHost, managerPort);
        managerBaseUrl = UriComponentsBuilder.fromHttpUrl(managerBaseUrl).pathSegment("task").build().encode().toString();
    }

    private long currentTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public void setStartTime(long taskId) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setStartTime")
                .queryParam("taskId", taskId)
                .queryParam("startTime", currentTimestamp())
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setSubmitTime(long taskId) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setSubmitTime")
                .queryParam("taskId", taskId)
                .queryParam("submitTime", currentTimestamp())
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setTotalTimeMs(long taskId, long totalTimeMs) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setTotalTimeMs")
                .queryParam("taskId", taskId)
                .queryParam("totalTimeMs", totalTimeMs)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void updateStatus(long taskId, Task.TaskStatus status) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("updateStatus")
                .queryParam("taskId", taskId)
                .queryParam("taskStatus", status)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setMapNum(long taskId, int mapNum) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setMapNum")
                .queryParam("taskId", taskId)
                .queryParam("mapNum", mapNum)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setApplicationId(long taskId, String applicationId) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setApplicationId")
                .queryParam("taskId", taskId)
                .queryParam("applicationId", applicationId)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setMRLogUrl(long taskId, String mrLogUrl) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setMRLogUrl")
                .queryParam("taskId", taskId)
                .queryParam("mrLogUrl", mrLogUrl)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setMRProgress(long taskId, int progress) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setMRProgress")
                .queryParam("taskId", taskId)
                .queryParam("mrProgress", progress)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setEstimatedByteSize(long taskId, long byteSize) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setEstimatedByteSize")
                .queryParam("taskId", taskId)
                .queryParam("byteSize", byteSize)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setMRTimeMs(long taskId, long ms) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setMRTimeMs")
                .queryParam("taskId", taskId)
                .queryParam("mrTimeMs", ms)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public void setRecordNum(long taskId, long recordNum) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("setRecordNum")
                .queryParam("taskId", taskId)
                .queryParam("recordNum", recordNum)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public long insert(Task task) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl).pathSegment("insert").build().encode().toString();
        MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
        header.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        header.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<Task> httpEntity = new HttpEntity<>(task, header);
        ResponseEntity<JsonResult> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
        return new BigInteger(RestUtils.parseResponseEntity(responseEntity).toString()).longValueExact();
    }

    @Override
    public void end(long taskId, Task.TaskStatus status) {
        String url = UriComponentsBuilder.fromHttpUrl(managerBaseUrl)
                .pathSegment("end")
                .queryParam("taskId", taskId)
                .queryParam("status", status)
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }
}
