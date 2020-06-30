package com.xiaohongshu.db.node.controller.advice;

import com.xiaohongshu.db.node.utils.CommandExecutor;
import com.xiaohongshu.db.share.vo.JsonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

@ControllerAdvice(basePackages = "com.xiaohongshu.db.node.controller")
public class ControllerResultHandler implements ResponseBodyAdvice<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public JsonResult handleException(Exception e) {
        LOG.error("Rest error.", e);
        return new JsonResult(e);
    }

    @Override
    public boolean supports(MethodParameter methodParameter, Class<? extends HttpMessageConverter<?>> aClass) {
        return true;
    }

    @Override
    public Object beforeBodyWrite(Object o, MethodParameter methodParameter, MediaType mediaType, Class<? extends HttpMessageConverter<?>> aClass, ServerHttpRequest serverHttpRequest, ServerHttpResponse serverHttpResponse) {
        if (o instanceof JsonResult) {
            return o;
        } else {
            return new JsonResult(o);
        }
    }

}
