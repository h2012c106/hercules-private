package com.xiaohongshu.db.hercules.mongodb.schema.manager;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOptionsConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MongoDBManager {

    private static final Log LOG = LogFactory.getLog(MongoDBManager.class);

    public static final String ID = "_id";

    private GenericOptions options;

    public MongoDBManager(GenericOptions options) {
        this.options = options;
    }

    private ServerAddress getServerAddress(String address) {
        String regex = "(\\S+):([0-9]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(address.trim());
        if (matcher.find()) {
            return new ServerAddress(matcher.group(1), Integer.parseInt(matcher.group(2)));
        } else {
            throw new ParseException("Unable to parse to '<host>:<port>' format connection address: " + address);
        }
    }

    private List<ServerAddress> getServerAddressList() {
        return Arrays.stream(options.getStringArray(MongoDBOptionsConf.CONNECTION, null))
                .map(this::getServerAddress)
                .collect(Collectors.toList());
    }

    public MongoClient getConnection() {
        String user = options.getString(MongoDBOptionsConf.USERNAME, null);
        String password = options.getString(MongoDBOptionsConf.PASSWORD, null);
        if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password)) {
            return new MongoClient(getServerAddressList());
        } else {
            String authdb = options.getString(MongoDBOptionsConf.AUTHDB,
                    options.getString(MongoDBOptionsConf.DATABASE, null));
            MongoCredential credential = MongoCredential.createCredential(user, authdb, password.toCharArray());
            return new MongoClient(getServerAddressList(), Collections.singletonList(credential));
        }
    }
}
