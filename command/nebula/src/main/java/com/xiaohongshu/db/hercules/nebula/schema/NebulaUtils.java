package com.xiaohongshu.db.hercules.nebula.schema;

import com.facebook.thrift.TException;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.*;
import com.vesoft.nebula.graph.ErrorCode;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.nebula.option.NebulaOptionsConf.*;

public final class NebulaUtils {

    private static final Log LOG = LogFactory.getLog(NebulaUtils.class);

    public static GraphClient getConnection(GenericOptions options) throws TException {
        GraphClient client = new GraphClientImpl(
                Arrays.stream(options.getTrimmedStringArray(ADDRESSES, null))
                        .map(HostAndPort::fromString)
                        .collect(Collectors.toList())
        );
        client.setUser(options.getString(USER, null));
        client.setPassword(options.getString(PASSWORD, null));
        checkReturnCode(client.connect(), "Failed to gain a nebula connection.");
        checkReturnCode(client.switchSpace(options.getString(SPACE, null)), "Failed to select a nebula space.");
        return client;
    }

    public static ResultSet executeQuery(GraphClient client, String sql) throws IOException {
        LOG.info("Nebula query sql: " + sql);
        try {
            return client.executeQuery(sql);
        } catch (ConnectionException | NGQLException | TException e) {
            throw new IOException("Execute sql error: " + sql, e);
        }
    }

    public static void executeUpdate(GraphClient client, String sql) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Nebula update sql: " + sql);
        }
        try {
            checkReturnCode(client.execute(sql), String.format("Failed to execute sql <%s>.", sql));
        } catch (RuntimeException e) {
            throw new IOException(e);
        }
    }

    private static void checkReturnCode(int code, String errorMessage) {
        if (ErrorCode.SUCCEEDED != code) {
            throw new RuntimeException(errorMessage + " (Code<" + code + ">)");
        }
    }

}
