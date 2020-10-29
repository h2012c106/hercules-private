package com.xiaohongshu.db.hercules.elasticsearchv6.mr;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.serder.json.ser.JsonWrapperSetterManager;

public class ElasticsearchJsonWrapperSetterManager extends JsonWrapperSetterManager {
    public ElasticsearchJsonWrapperSetterManager() {
        super(DataSourceRole.TARGET);
    }
}
