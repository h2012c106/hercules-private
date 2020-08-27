package com.xiaohongshu.db.share.vo;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.exception.ExceptionUtils;

@NoArgsConstructor
@Getter
@Setter
public class JsonResult {

    private boolean success;
    private String errorMessage;
    private String errorStack;
    private Object content;

    public JsonResult(Object content) {
        this.success = true;
        this.errorMessage = null;
        this.errorStack = null;
        this.content = content;
    }

    public JsonResult(Exception e) {
        this.success = false;
        this.errorMessage = e.getMessage();
        this.errorStack = ExceptionUtils.getStackTrace(e);
        this.content = null;
    }
}
