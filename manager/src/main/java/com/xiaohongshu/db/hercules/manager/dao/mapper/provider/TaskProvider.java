package com.xiaohongshu.db.hercules.manager.dao.mapper.provider;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.share.entity.Task;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TaskProvider extends BaseProvider<Task> {
    @Override
    protected String getTableName() {
        return "task";
    }

    /**
     * Task无patch，对用户仅可读，调用patch方法的只可能是manager自己，map内容严格把控
     *
     * @return
     */
    @Override
    protected Set<String> unallowedPatchColumn() {
        return null;
    }

    @Override
    protected Set<String> unallowedInsertColumn() {
        return Sets.newHashSet(
                "submitTime",
                "startTime",
                "totalTimeMs",
                "mrTimeMs",
                "mapNum",
                "recordNum",
                "estimatedByteSize",
                "applicationId",
                "mrLogUrl",
                "mrProgress",
                "status"
        );
    }

    public String updateStatus(long taskId, Task.TaskStatus status) {
        List<Task.TaskStatus> preStatusList = Task.TaskStatus.getPreStatusList(status);
        List<String> statusWhereList = preStatusList.stream()
                .map(preStatus -> String.format("`status` = '%s'", preStatus.name()))
                .collect(Collectors.toList());
        String res = "update `" + getTableName() + "` set `status` = '" + status.name() + "' where `id` = #{taskId}";
        if (statusWhereList.size() > 0) {
            res += " and ( " + StringUtils.join(statusWhereList, " or ") + " )";
        }
        return res + ";";
    }
}
