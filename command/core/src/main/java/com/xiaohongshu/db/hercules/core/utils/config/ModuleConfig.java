package com.xiaohongshu.db.hercules.core.utils.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.Map;

@Data
public class ModuleConfig {

    private static final String JAR_NAME_PROP = "jar";
    private static final String ASSEMBLY_CLASS_NAME_PROP = "assembly-class";

    private String jar;
    private String assemblyClass;

    public static ModuleConfig get(Map<String, String> map) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(map, ModuleConfig.class);
    }

}
