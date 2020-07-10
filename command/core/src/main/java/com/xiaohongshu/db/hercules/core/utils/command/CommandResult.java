package com.xiaohongshu.db.hercules.core.utils.command;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CommandResult {
    private int code;
    private String data;
    private String error;

    public CommandResult(int code, String data, String error) {
        this.code = code;
        this.data = data;
        this.error = error;
    }

    public int getCode() {
        return code;
    }

    public String getData() {
        return data;
    }

    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("code", code)
                .append("data", data)
                .append("error", error)
                .toString();
    }
}
