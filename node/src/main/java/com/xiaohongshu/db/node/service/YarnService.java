package com.xiaohongshu.db.node.service;

import java.util.List;

public interface YarnService {

    public void kill(String applicationId) throws Exception;

    public List<String> findApplicationIdByName(String name) throws Exception;

}
