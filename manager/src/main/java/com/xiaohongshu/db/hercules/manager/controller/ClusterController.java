package com.xiaohongshu.db.hercules.manager.controller;

import com.xiaohongshu.db.share.entity.Cluster;
import com.xiaohongshu.db.hercules.manager.service.ClusterService;
import com.xiaohongshu.db.share.utils.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/cluster")
public class ClusterController {

    @Autowired
    private ClusterService clusterService;

    @PostMapping("/insert")
    public long insert(@RequestBody @Validated Cluster cluster) {
        return clusterService.insert(cluster);
    }

    @PatchMapping("/patch")
    public void patch(@RequestBody Map<String, Object> map) {
        if (!map.containsKey(Constant.ID_COL_NAME)) {
            throw new NullPointerException("Unfilled cluster id: " + map);
        }
        clusterService.patch(map);
    }

    @DeleteMapping("/delete")
    public void delete(@RequestParam long id) {
        clusterService.delete(id);
    }

    @GetMapping("/findOne")
    public Cluster find(Cluster cluster) {
        if (cluster.getId() == null && cluster.getName() == null) {
            throw new NullPointerException("Need at least one parameter to find: 'id' or 'name'.");
        } else {
            return clusterService.findOne(cluster);
        }
    }

    @GetMapping("/findAll")
    public List<Cluster> find() {
        return clusterService.findAll();
    }

}
