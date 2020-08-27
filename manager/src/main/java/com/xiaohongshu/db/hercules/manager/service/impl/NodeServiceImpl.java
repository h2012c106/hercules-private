package com.xiaohongshu.db.hercules.manager.service.impl;

import com.xiaohongshu.db.hercules.manager.dao.mapper.NodeMapper;
import com.xiaohongshu.db.hercules.manager.exception.NotFoundException;
import com.xiaohongshu.db.hercules.manager.service.NodeService;
import com.xiaohongshu.db.share.entity.Node;
import com.xiaohongshu.db.share.utils.Constant;
import com.xiaohongshu.db.share.utils.RestUtils;
import com.xiaohongshu.db.share.vo.JsonResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;

@Service
public class NodeServiceImpl implements NodeService {

    @Autowired
    private NodeMapper nodeMapper;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public void updateStatus(long id, Node.NodeStatus status) {
        int row = nodeMapper.updateStatus(id, status.name());
        if (row != 1) {
            throw new RuntimeException("Unexpected updated row: " + row);
        }
    }

    @Override
    public void ping(long id) {
        Node node = findOne(new Node(id));
        if (node.getStatus() == Node.NodeStatus.OFF) {
            throw new RuntimeException(String.format("The node [%s] is closed.", node.getHost()));
        }
        String url = UriComponentsBuilder.fromHttpUrl(Constant.nodeRestBaseUrl(node.getHost(), node.getPort()))
                .pathSegment("node")
                .pathSegment("ping")
                .build()
                .encode()
                .toString();
        ResponseEntity<JsonResult> responseEntity = restTemplate.getForEntity(url, JsonResult.class);
        RestUtils.validateResponseEntity(responseEntity);
    }

    @Override
    public long insert(Node entity) {
        nodeMapper.insert(entity);
        return entity.getId();
    }

    @Override
    public void delete(long id) {
        nodeMapper.delete(id);
    }

    @Override
    public Node findOne(Node entity) {
        Node res = nodeMapper.findOne(entity);
        if (res == null) {
            throw new NotFoundException("No node with node info: " + entity);
        } else {
            return res;
        }
    }

    @Override
    public List<Node> findAll() {
        return nodeMapper.findAll();
    }

    @Override
    public void patch(Map<String, Object> map) {
        int row = nodeMapper.patch(map);
        if (row != 1) {
            throw new RuntimeException("Unexpected updated row: " + row);
        }
    }
}
