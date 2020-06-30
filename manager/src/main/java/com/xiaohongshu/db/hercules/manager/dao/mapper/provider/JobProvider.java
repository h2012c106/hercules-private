package com.xiaohongshu.db.hercules.manager.dao.mapper.provider;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.share.entity.Job;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JobProvider extends BaseProvider<Job> {
    @Override
    protected String getTableName() {
        return "job";
    }

    @Override
    protected Set<String> unallowedPatchColumn() {
        return Sets.newHashSet("runningTaskId");
    }

    @Override
    protected Set<String> unallowedInsertColumn() {
        return Sets.newHashSet("runningTaskId");
    }

    @Override
    protected Map<String, String> humpToUnderlineException() {
        Map<String, String> res = new HashMap<>(2);
        res.put("dParam", "D_param");
        res.put("dTemplateParam", "D_template_param");
        return res;
    }
}
