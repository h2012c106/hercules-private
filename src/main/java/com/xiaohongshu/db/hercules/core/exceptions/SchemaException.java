package com.xiaohongshu.db.hercules.core.exceptions;

public class SchemaException extends RuntimeException {
    public SchemaException() {
    }

    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchemaException(Throwable cause) {
        super(cause);
    }
}
