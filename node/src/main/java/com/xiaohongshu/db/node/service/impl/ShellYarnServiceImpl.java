package com.xiaohongshu.db.node.service.impl;

import com.xiaohongshu.db.node.service.YarnService;
import com.xiaohongshu.db.node.utils.CommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service
public class ShellYarnServiceImpl implements YarnService {

    private static final Logger LOG = LoggerFactory.getLogger(ShellYarnServiceImpl.class);

    @Autowired
    @Qualifier("cachedPool")
    private ExecutorService commonCommandPool;

    @Override
    public void kill(String applicationId) throws Exception {
        String command = "yarn application -kill " + applicationId;
        Future<CommandExecutor.CommandExecutorResult> future = commonCommandPool.submit(new CommandExecutor(command));
        CommandExecutor.CommandExecutorResult result = future.get();
        if (result.getExitCode() != 0) {
            throw new RuntimeException("Kill failed. Message: " + result.getMessage());
        }
    }

    @Override
    public List<String> findApplicationIdByName(String name) throws Exception {
        String command = String.format("yarn application -list -appStates RUNNING 2>/dev/null | awk '$2 == \"%s\" {print $1}'", name);
        Future<CommandExecutor.CommandExecutorResult> future = commonCommandPool.submit(new CommandExecutor(command));
        try {
            CommandExecutor.CommandExecutorResult result = future.get();
            if (result.getExitCode() != 0) {
                throw new IOException("Find apllication id failed.\n" + result.getMessage());
            } else {
                return Arrays.stream(result.getMessage().split("\\s+"))
                        .map(String::trim)
                        .filter(applicationId -> applicationId.length() > 0)
                        .collect(Collectors.toList());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Find apllication id failed.", e);
        }
    }
}
