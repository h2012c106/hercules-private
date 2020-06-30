package com.xiaohongshu.db.node.config;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

@Configuration
public class NodeSpringConfig {

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder){
        return builder.build();
    }

    @Bean
    public ExecutorService cachedPool(){
        return new CachedPoolBean();
    }

    private static class CachedPoolBean implements ExecutorService, DisposableBean{

        private static final String COMMON_COMMAND_THREAD_PREFIX = "COMMON-COMMAND-THREAD-";

        private final ExecutorService commonCommandPool
                = Executors.newCachedThreadPool(new CustomizableThreadFactory(COMMON_COMMAND_THREAD_PREFIX));

        @Override
        public void shutdown() {
            commonCommandPool.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return commonCommandPool.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return commonCommandPool.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return commonCommandPool.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return commonCommandPool.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return commonCommandPool.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return commonCommandPool.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return commonCommandPool.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return commonCommandPool.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return commonCommandPool.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return commonCommandPool.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return commonCommandPool.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            commonCommandPool.execute(command);
        }

        @Override
        public void destroy() throws Exception {
            commonCommandPool.shutdown();
            commonCommandPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

}
