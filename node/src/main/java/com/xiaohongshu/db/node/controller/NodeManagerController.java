package com.xiaohongshu.db.node.controller;

import com.xiaohongshu.db.node.service.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/rest/node/node")
public class NodeManagerController {

    @Autowired
    private NodeService nodeService;

    @GetMapping("/ping")
    public void ping() throws Exception {
        Exception e = nodeService.ping();
        if (e != null) {
            throw e;
        }
    }

}
