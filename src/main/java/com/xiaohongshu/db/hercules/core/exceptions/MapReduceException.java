package com.xiaohongshu.db.hercules.core.exceptions;

public class MapReduceException extends RuntimeException {
    public MapReduceException() {
    }

    public MapReduceException(String message) {
        super(message);
    }

    public MapReduceException(String message, Throwable cause) {
        super(message, cause);
    }

    public MapReduceException(Throwable cause) {
        super(cause);
    }
}
