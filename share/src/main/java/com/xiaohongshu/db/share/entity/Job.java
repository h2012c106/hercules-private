package com.xiaohongshu.db.share.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

import javax.validation.constraints.NotEmpty;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class Job {

    public Job(Long id) {
        this.id = id;
    }

    private Long id;

    @NotEmpty
    private String name;

    private String desc;

    private DataSource source;

    private DataSource target;

    @NotEmpty
    private Map<String, String> sourceParam;

    @NotEmpty
    private Map<String, String> targetParam;

    @NotEmpty
    private Map<String, String> commonParam;

    @JsonProperty("dParam")
    private Map<String, String> dParam;

    private Map<String, String> sourceTemplateParam;

    private Map<String, String> targetTemplateParam;

    private Map<String, String> commonTemplateParam;

    @JsonProperty("dTemplateParam")
    private Map<String, String> dTemplateParam;

    private int runningTaskId;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return Objects.equal(id, job.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @SneakyThrows
    @Override
    public String toString() {
        return new ObjectMapper().writeValueAsString(this);
    }

}
