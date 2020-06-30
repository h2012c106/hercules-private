package com.xiaohongshu.db.hercules.manager.controller;

import com.xiaohongshu.db.hercules.manager.service.NodeService;
import com.xiaohongshu.db.share.entity.Node;
import com.xiaohongshu.db.share.utils.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/node")
public class NodeController {

    @Autowired
    private NodeService nodeService;

    @PostMapping("/insert")
    public long insert(@RequestBody @Validated Node node) {
        return nodeService.insert(node);
    }

    @PatchMapping("/patch")
    public void patch(@RequestBody Map<String, Object> map) {
        if (!map.containsKey(Constant.ID_COL_NAME)) {
            throw new NullPointerException("Unfilled node id: " + map);
        }
        nodeService.patch(map);
    }

    @PatchMapping("/updateStatus")
    public void updateStatus(@RequestParam long id, @RequestParam Node.NodeStatus status) {
        nodeService.updateStatus(id, status);
    }

    @DeleteMapping("/delete")
    public void delete(@RequestParam long id) {
        nodeService.delete(id);
    }

    @GetMapping("/findOne")
    public Node findOne(Node node) {
        if (node.getId() == null) {
            throw new NullPointerException("Need parameter: 'id'.");
        }
        return nodeService.findOne(node);
    }

    @GetMapping("/findAll")
    public List<Node> findAll() {
        return nodeService.findAll();
    }

    @GetMapping("/ping")
    public void ping(long id) {
        nodeService.ping(id);
    }

}
