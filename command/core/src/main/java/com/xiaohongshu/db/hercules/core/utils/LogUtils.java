package com.xiaohongshu.db.hercules.core.utils;

/**
 * 由于hadoop jar死活读不了我的log4j.properties，只能来硬的
 */
public final class LogUtils {

    private static String LOG_PATTERN = "[%-d{yyyy-MM-dd HH:mm:ss}]-[%t-%p]-[%C-%M(%L)]: %m%n";

    public static void configureLog4J() {
        // 把hadoop的给扔了
        // Logger.getRootLogger().removeAppender("CONSOLE");
        // 用自己的
        // Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout(LOG_PATTERN)));
    }

}
