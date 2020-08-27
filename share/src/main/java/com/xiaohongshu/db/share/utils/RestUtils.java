package com.xiaohongshu.db.share.utils;

import com.xiaohongshu.db.share.vo.JsonResult;
import org.springframework.http.ResponseEntity;

public final class RestUtils {

    public static void validateResponseEntity(ResponseEntity<JsonResult> responseEntity) {
        if (!responseEntity.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException(String.format("Rest call failed. Code [%d], Reason [%s]", responseEntity.getStatusCode().value(), responseEntity.getStatusCode().getReasonPhrase()));
        }
        if (!responseEntity.getBody().isSuccess()) {
            throw new RuntimeException(responseEntity.getBody().getErrorStack());
        }
    }

    public static Object parseResponseEntity(ResponseEntity<JsonResult> responseEntity) {
        return responseEntity.getBody().getContent();
    }

}
