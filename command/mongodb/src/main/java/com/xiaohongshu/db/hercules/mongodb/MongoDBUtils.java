package com.xiaohongshu.db.hercules.mongodb;

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

public final class MongoDBUtils {

    private static final Log LOG = LogFactory.getLog(MongoDBUtils.class);

    public static final String ID = "_id";

    private static ServerAddress getServerAddress(String address) {
        String regex = "(\\S+):([0-9]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(address.trim());
        if (matcher.find()) {
            return new ServerAddress(matcher.group(1), Integer.parseInt(matcher.group(2)));
        } else {
            throw new ParseException("Unable to parse to '<host>:<port>' format connection address: " + address);
        }
    }

    private static List<ServerAddress> getServerAddressList(GenericOptions options) {
        return Arrays.stream(options.getTrimmedStringArray(MongoDBOptionsConf.CONNECTION, null))
                .map(MongoDBUtils::getServerAddress)
                .collect(Collectors.toList());
    }

    public static MongoClient getConnection(GenericOptions options) {
        String user = options.getString(MongoDBOptionsConf.USERNAME, null);
        String password = options.getString(MongoDBOptionsConf.PASSWORD, null);
        if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password)) {
            return new MongoClient(getServerAddressList(options));
        } else {
            String authdb = options.getString(MongoDBOptionsConf.AUTHDB,
                    options.getString(MongoDBOptionsConf.DATABASE, null));
            MongoCredential credential = MongoCredential.createCredential(user, authdb, password.toCharArray());
            return new MongoClient(getServerAddressList(options), Collections.singletonList(credential));
        }
    }
}
