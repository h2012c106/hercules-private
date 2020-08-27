package com.xiaohongshu.db.hercules.manager.service;

import com.xiaohongshu.db.share.entity.Node;

public interface NodeService extends BaseService<Node> {

    public void updateStatus(long id, Node.NodeStatus status);

    public void ping(long id);

}
