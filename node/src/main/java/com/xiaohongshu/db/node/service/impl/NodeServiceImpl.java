package com.xiaohongshu.db.node.service.impl;

import com.xiaohongshu.db.node.service.NodeService;
import com.xiaohongshu.db.node.utils.CommandExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Service
public class NodeServiceImpl implements NodeService {

    @Autowired
    @Qualifier("cachedPool")
    private ExecutorService commonCommandPool;

    @Override
    public Exception ping() {
        String command = "which hercules";
        Future<CommandExecutor.CommandExecutorResult> future = commonCommandPool.submit(new CommandExecutor(command));
        CommandExecutor.CommandExecutorResult result = null;
        try {
            result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            return e;
        }
        if (result.getExitCode() != 0) {
            return new RuntimeException("Kill failed. Message: " + result.getMessage());
        } else {
            return null;
        }
    }
}
